package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotBaseFormat              = uint16(1)
	snapshotBaseHeaderBytes         = 272
	snapshotBaseRelationFixedBytes  = 48
	snapshotBaseRelationDigestBytes = sha256.Size
	MaxSnapshotBaseCertificateBytes = snapshotBaseHeaderBytes +
		MaxStateEnvelopeBytes + MaxStaticBootstrapEnvelopeBytes +
		replication.MaxCollectionBytes + snapshotBaseRelationDigestBytes +
		replication.MaxRelationsPerBundle*(snapshotBaseRelationFixedBytes+
			replication.MaxIdentityBytes) + recordChecksumLen
)

var (
	snapshotBaseMagic          = [8]byte{'V', 'D', 'B', 'R', 'B', 'A', 'S', 'E'}
	snapshotBaseChecksumDomain = []byte(
		"vibedb/replicated-state/snapshot-base-certificate\x00",
	)
	snapshotBaseIdentityDomain = []byte(
		"vibedb/replicated-state/snapshot-base-identity\x00",
	)
	seededSnapshotManifestDomain = []byte(
		"vibedb/replicated-state/seeded-snapshot-manifest\x00",
	)
)

// SnapshotBaseCertificate is the small authenticated bridge between an
// already verified collection image and an immutable Raft WAL base. The image
// may be a streamed artifact or an exclusively owned no-copy seed; the
// certificate never contains collection rows. StaticBootstrap is the original
// index-one identity required to reopen the replicated apply layer.
type SnapshotBaseCertificate struct {
	Manifest        SnapshotArtifactManifest
	StaticBootstrap *pb.Snapshot
	Digest          [sha256.Size]byte
}

// BuildSnapshotBase constructs the bounded Raft snapshot certificate for a
// completely verified image manifest. Bulk state remains in the staged
// collection files. The returned protobuf is detached from every input.
func BuildSnapshotBase(
	manifest SnapshotArtifactManifest,
	staticBootstrap *pb.Snapshot,
) (*pb.Snapshot, error) {
	bootstrapBytes, bootstrapDigest, err := validateBootstrap(staticBootstrap)
	if err != nil {
		return nil, fmt.Errorf("%w: static bootstrap", ErrSnapshotBase)
	}
	return buildSnapshotBaseFromCanonicalBootstrap(
		manifest,
		bootstrapBytes,
		bootstrapDigest,
	)
}

func buildSnapshotBaseFromCanonicalBootstrap(
	manifest SnapshotArtifactManifest,
	bootstrapBytes []byte,
	bootstrapDigest [sha256.Size]byte,
) (*pb.Snapshot, error) {
	if err := validateSnapshotBaseManifest(manifest); err != nil {
		return nil, fmt.Errorf("%w: manifest: %v", ErrSnapshotBase, err)
	}
	if len(bootstrapBytes) == 0 || len(bootstrapBytes) > MaxStaticBootstrapEnvelopeBytes ||
		bootstrapDigest != manifest.State.BootstrapDigest {
		return nil, fmt.Errorf("%w: static bootstrap", ErrSnapshotBase)
	}
	stateBytes, err := AppendState(nil, manifest.State)
	if err != nil {
		return nil, fmt.Errorf("%w: state: %v", ErrSnapshotBase, err)
	}
	relationBodyBytes, err := snapshotBaseRelationBodyBytes(manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: relation manifest: %v", ErrSnapshotBase, err)
	}
	total := snapshotBaseHeaderBytes + len(stateBytes) + len(bootstrapBytes) +
		len(manifest.UserCollection) + relationBodyBytes + recordChecksumLen
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
	var flags uint16
	if manifest.Seeded {
		flags |= 1
	}
	if manifest.Bundle {
		flags |= 2
	}
	binary.LittleEndian.PutUint16(result[26:28], flags)
	binary.LittleEndian.PutUint32(result[28:32], manifest.TargetChunkBytes)
	binary.LittleEndian.PutUint64(result[32:40], manifest.Chunks)
	binary.LittleEndian.PutUint64(result[40:48], manifest.SystemRows)
	binary.LittleEndian.PutUint64(result[48:56], manifest.UserRows)
	binary.LittleEndian.PutUint64(result[56:64], manifest.PayloadBytes)
	binary.LittleEndian.PutUint64(result[64:72], manifest.EncodedBytes)
	binary.LittleEndian.PutUint64(result[72:80], uint64(len(manifest.Relations)))
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
	cursor += copy(result[cursor:], manifest.UserCollection)
	if manifest.Bundle {
		copy(result[cursor:cursor+sha256.Size], manifest.RelationManifestDigest[:])
		cursor += sha256.Size
		for i := range manifest.Relations {
			relation := &manifest.Relations[i]
			binary.LittleEndian.PutUint16(result[cursor:cursor+2], uint16(relation.Relation))
			result[cursor+2] = byte(relation.Kind)
			binary.LittleEndian.PutUint16(result[cursor+4:cursor+6], uint16(len(relation.Collection)))
			binary.LittleEndian.PutUint64(result[cursor+8:cursor+16], relation.Rows)
			copy(result[cursor+16:cursor+48], relation.ImageDigest[:])
			cursor += snapshotBaseRelationFixedBytes
			cursor += copy(result[cursor:], relation.Collection)
		}
	}
	if cursor != len(result)-recordChecksumLen {
		return nil, fmt.Errorf("%w: relation body geometry", ErrSnapshotBase)
	}
	sealRecord(result, snapshotBaseChecksumDomain)
	index, term := manifest.State.Applied, manifest.State.LastTerm
	return &pb.Snapshot{Data: result, Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: cloneConfState(manifest.State.ConfState),
	}}, nil
}

func snapshotBaseRelationBodyBytes(manifest SnapshotArtifactManifest) (int, error) {
	if !manifest.Bundle {
		if len(manifest.Relations) != 0 ||
			manifest.RelationManifestDigest != ([sha256.Size]byte{}) {
			return 0, ErrSnapshotBase
		}
		return 0, nil
	}
	if len(manifest.Relations) == 0 ||
		len(manifest.Relations) > replication.MaxRelationsPerBundle {
		return 0, ErrSnapshotBase
	}
	total := snapshotBaseRelationDigestBytes
	for i := range manifest.Relations {
		nameBytes := len(manifest.Relations[i].Collection)
		if nameBytes == 0 || nameBytes > replication.MaxIdentityBytes ||
			total > MaxSnapshotBaseCertificateBytes-snapshotBaseRelationFixedBytes-nameBytes {
			return 0, ErrSnapshotBase
		}
		total += snapshotBaseRelationFixedBytes + nameBytes
	}
	return total, nil
}

// BuildSnapshotBaseForManifest binds a manifest produced from a pinned Machine
// snapshot to that Machine's immutable index-one bootstrap. The Machine may
// continue applying after the snapshot was captured. Only the immutable
// bootstrap is read here. This performs no collection scan.
func (m *Machine) BuildSnapshotBaseForManifest(
	manifest SnapshotArtifactManifest,
) (*pb.Snapshot, error) {
	if m == nil {
		return nil, ErrSnapshotBase
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return nil, err
	}
	return buildSnapshotBaseFromCanonicalBootstrap(
		manifest,
		m.bootstrap,
		m.bootstrapDigest,
	)
}

// BuildBundleSnapshotBase certifies every fixed relation image and the hidden
// state at one coherent DatabaseSnapshot cut. It carries no relation rows and
// therefore performs no large copy; snapshot transport moves the already
// durable member images separately under the same checkpoint-group cut.
func (m *Machine) BuildBundleSnapshotBase() (
	result *pb.Snapshot,
	resultManifest SnapshotArtifactManifest,
	resultErr error,
) {
	if m == nil {
		return nil, SnapshotArtifactManifest{}, ErrSnapshotBase
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return nil, SnapshotArtifactManifest{}, err
	}
	if !m.initialized || len(m.relations) < 2 {
		return nil, SnapshotArtifactManifest{}, ErrSnapshotBase
	}
	cut, err := durable.SnapshotCollections(m.members)
	if err != nil {
		return nil, SnapshotArtifactManifest{}, m.fail(err)
	}
	defer func() {
		if closeErr := cut.Close(); closeErr != nil {
			result = nil
			resultManifest = SnapshotArtifactManifest{}
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	system, ok := cut.Collection(systemCollectionName)
	if !ok || system == nil {
		return nil, SnapshotArtifactManifest{}, m.fail(ErrInconsistentSnapshot)
	}
	state, present, sessions, slots, err := scanSessionSystemSnapshot(
		system, m.options.MaxSessions, m.options.RetryWindow,
	)
	if err != nil || !present || sessions != state.SessionCount ||
		slots != state.SessionSlotCount || !equalState(state, m.state) {
		return nil, SnapshotArtifactManifest{}, m.fail(errors.Join(ErrInconsistentSnapshot, err))
	}
	manifest := SnapshotArtifactManifest{
		State: cloneState(state), UserCollection: []byte(m.relations[0].name),
		Bundle: true, RelationManifestDigest: m.manifestDigest,
		SystemRows: system.Len(), Relations: make([]SnapshotArtifactRelation, len(m.relations)),
	}
	for i := range m.relations {
		relation := &m.relations[i]
		snapshot, exists := cut.Collection(relation.name)
		if !exists || snapshot == nil || snapshot.Generation() == 0 {
			return nil, SnapshotArtifactManifest{}, m.fail(ErrInconsistentSnapshot)
		}
		digest, digestErr := openedRelationImageDigest(relation, snapshot)
		if digestErr != nil {
			return nil, SnapshotArtifactManifest{}, m.fail(digestErr)
		}
		manifest.Relations[i] = SnapshotArtifactRelation{
			Relation: relation.id, Kind: relation.kind,
			Collection: []byte(relation.name), Rows: snapshot.Len(), ImageDigest: digest,
		}
		if manifest.UserRows > math.MaxUint64-snapshot.Len() {
			return nil, SnapshotArtifactManifest{}, ErrSnapshotBase
		}
		manifest.UserRows += snapshot.Len()
	}
	manifest.ImageDigest = canonicalBundleImageDigest(manifest.Relations)
	stateEnvelope, err := AppendState(nil, manifest.State)
	if err != nil {
		return nil, SnapshotArtifactManifest{}, err
	}
	manifest.Digest = bundleSnapshotManifestDigest(
		stateEnvelope, manifest.RelationManifestDigest, manifest.SystemRows,
		manifest.Relations, manifest.ImageDigest,
	)
	base, err := buildSnapshotBaseFromCanonicalBootstrap(
		manifest, m.bootstrap, m.bootstrapDigest,
	)
	if err != nil {
		return nil, SnapshotArtifactManifest{}, err
	}
	return base, cloneSnapshotArtifactManifest(manifest), nil
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
		binary.LittleEndian.Uint16(data[26:28])&^uint16(3) != 0 ||
		!verifyRecord(data, snapshotBaseChecksumDomain) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: certificate envelope", ErrSnapshotBase)
	}
	stateBytes := uint64(binary.LittleEndian.Uint32(data[16:20]))
	bootstrapBytes := uint64(binary.LittleEndian.Uint32(data[20:24]))
	userBytes := uint64(binary.LittleEndian.Uint16(data[24:26]))
	flags := binary.LittleEndian.Uint16(data[26:28])
	relationCount := binary.LittleEndian.Uint64(data[72:80])
	bodyBytes := uint64(len(data) - snapshotBaseHeaderBytes - recordChecksumLen)
	if stateBytes == 0 || bootstrapBytes == 0 || userBytes == 0 ||
		stateBytes > MaxStateEnvelopeBytes ||
		bootstrapBytes > MaxStaticBootstrapEnvelopeBytes ||
		userBytes > replication.MaxCollectionBytes || relationCount > replication.MaxRelationsPerBundle ||
		stateBytes+bootstrapBytes+userBytes > bodyBytes ||
		(relationCount == 0) != (flags&2 == 0) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: certificate geometry", ErrSnapshotBase)
	}
	relationBodyBytes := bodyBytes - stateBytes - bootstrapBytes - userBytes
	if flags&2 != 0 {
		// Prove the aggregate relation-descriptor geometry before allocating the
		// descriptor slice or cloning any relation name. The count is already
		// bounded above, so these products cannot overflow uint64.
		minimum := uint64(snapshotBaseRelationDigestBytes) + relationCount*
			uint64(snapshotBaseRelationFixedBytes+1)
		maximum := uint64(snapshotBaseRelationDigestBytes) + relationCount*
			uint64(snapshotBaseRelationFixedBytes+replication.MaxIdentityBytes)
		if relationCount < 2 || userBytes > replication.MaxIdentityBytes ||
			relationBodyBytes < minimum || relationBodyBytes > maximum {
			return SnapshotBaseCertificate{}, fmt.Errorf(
				"%w: aggregate relation geometry", ErrSnapshotBase,
			)
		}
	} else if relationBodyBytes != 0 {
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
		Seeded:           flags&1 != 0,
		Bundle:           flags&2 != 0,
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
	if manifest.Bundle {
		cursor = userEnd
		if relationBodyBytes < sha256.Size {
			return SnapshotBaseCertificate{}, fmt.Errorf("%w: relation manifest geometry", ErrSnapshotBase)
		}
		copy(manifest.RelationManifestDigest[:], data[cursor:cursor+sha256.Size])
		cursor += sha256.Size
		manifest.Relations = make([]SnapshotArtifactRelation, int(relationCount))
		for i := range manifest.Relations {
			if cursor > len(data)-recordChecksumLen-snapshotBaseRelationFixedBytes {
				return SnapshotBaseCertificate{}, fmt.Errorf("%w: relation descriptor", ErrSnapshotBase)
			}
			fixed := data[cursor : cursor+snapshotBaseRelationFixedBytes]
			nameBytes := int(binary.LittleEndian.Uint16(fixed[4:6]))
			if fixed[3] != 0 || binary.LittleEndian.Uint16(fixed[6:8]) != 0 ||
				nameBytes == 0 || nameBytes > replication.MaxIdentityBytes {
				return SnapshotBaseCertificate{}, fmt.Errorf("%w: relation descriptor", ErrSnapshotBase)
			}
			cursor += snapshotBaseRelationFixedBytes
			if cursor > len(data)-recordChecksumLen-nameBytes {
				return SnapshotBaseCertificate{}, fmt.Errorf("%w: relation name", ErrSnapshotBase)
			}
			relation := &manifest.Relations[i]
			relation.Relation = replication.RelationID(binary.LittleEndian.Uint16(fixed[0:2]))
			relation.Kind = RelationKind(fixed[2])
			relation.Rows = binary.LittleEndian.Uint64(fixed[8:16])
			copy(relation.ImageDigest[:], fixed[16:48])
			relation.Collection = bytes.Clone(data[cursor : cursor+nameBytes])
			cursor += nameBytes
		}
		if cursor != len(data)-recordChecksumLen {
			return SnapshotBaseCertificate{}, fmt.Errorf("%w: trailing relation manifest", ErrSnapshotBase)
		}
	} else if stateBytes+bootstrapBytes+userBytes != bodyBytes {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: certificate geometry", ErrSnapshotBase)
	}
	if err := validateSnapshotBaseManifest(manifest); err != nil {
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

func validateSnapshotBaseManifest(manifest SnapshotArtifactManifest) error {
	if manifest.Bundle {
		return validateBundleSnapshotManifest(manifest)
	}
	if len(manifest.Relations) != 0 ||
		manifest.RelationManifestDigest != ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: relation manifest without bundle flag", ErrSnapshotBase)
	}
	if !manifest.Seeded {
		return validateExpectedSnapshotArtifact(manifest)
	}
	if err := validateState(manifest.State); err != nil ||
		manifest.State.LastKind != RecordImportedSnapshot ||
		manifest.State.SessionCount != 0 || manifest.State.SessionSlotCount != 0 ||
		len(manifest.UserCollection) == 0 ||
		len(manifest.UserCollection) > replication.MaxCollectionBytes ||
		!utf8.Valid(manifest.UserCollection) ||
		bytes.IndexByte(manifest.UserCollection, 0) >= 0 ||
		bytes.Equal(manifest.UserCollection, []byte(systemCollectionName)) ||
		manifest.TargetChunkBytes != 0 || manifest.Chunks != 0 ||
		manifest.SystemRows != 0 || manifest.PayloadBytes != 0 ||
		manifest.EncodedBytes != 0 || manifest.HeaderDigest != ([sha256.Size]byte{}) ||
		manifest.LastChunkDigest != ([sha256.Size]byte{}) ||
		manifest.ImageDigest == ([sha256.Size]byte{}) ||
		manifest.Digest == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: seeded manifest", ErrSnapshotBase)
	}
	stateEnvelope, err := AppendState(nil, manifest.State)
	if err != nil {
		return fmt.Errorf("%w: seeded state", ErrSnapshotBase)
	}
	wantDataChain, err := dataChainSeedDigest(
		manifest.State.ApplyContractDigest, manifest.ImageDigest,
	)
	if err != nil || wantDataChain != manifest.State.DataChainDigest {
		return fmt.Errorf("%w: seeded image differs from state chain", ErrSnapshotBase)
	}
	if manifest.Digest != seededSnapshotManifestDigest(
		stateEnvelope, manifest.UserCollection, manifest.ImageDigest, manifest.UserRows,
	) {
		return fmt.Errorf("%w: seeded manifest identity", ErrSnapshotBase)
	}
	return nil
}

func validateBundleSnapshotManifest(manifest SnapshotArtifactManifest) error {
	if manifest.Seeded || len(manifest.Relations) < 2 ||
		len(manifest.Relations) > replication.MaxRelationsPerBundle ||
		manifest.RelationManifestDigest == ([sha256.Size]byte{}) ||
		manifest.TargetChunkBytes != 0 || manifest.Chunks != 0 ||
		manifest.PayloadBytes != 0 || manifest.EncodedBytes != 0 ||
		manifest.HeaderDigest != ([sha256.Size]byte{}) ||
		manifest.LastChunkDigest != ([sha256.Size]byte{}) ||
		manifest.ImageDigest == ([sha256.Size]byte{}) ||
		manifest.Digest == ([sha256.Size]byte{}) ||
		manifest.SystemRows != manifest.State.SessionCount+manifest.State.SessionSlotCount+1 ||
		len(manifest.UserCollection) == 0 ||
		len(manifest.UserCollection) > replication.MaxCollectionBytes ||
		!utf8.Valid(manifest.UserCollection) ||
		bytes.IndexByte(manifest.UserCollection, 0) >= 0 ||
		bytes.Equal(manifest.UserCollection, []byte(systemCollectionName)) {
		return fmt.Errorf("%w: bundle manifest", ErrSnapshotBase)
	}
	if err := validateState(manifest.State); err != nil {
		return fmt.Errorf("%w: bundle state: %v", ErrSnapshotBase, err)
	}
	seenNames := make(map[string]struct{}, len(manifest.Relations))
	var rows uint64
	for i := range manifest.Relations {
		relation := &manifest.Relations[i]
		if relation.Relation != replication.RelationID(i+1) ||
			(relation.Kind != RelationJSON && relation.Kind != RelationGlobalIndex) ||
			len(relation.Collection) == 0 ||
			len(relation.Collection) > replication.MaxIdentityBytes ||
			!utf8.Valid(relation.Collection) ||
			bytes.IndexByte(relation.Collection, 0) >= 0 ||
			bytes.Equal(relation.Collection, []byte(systemCollectionName)) ||
			relation.ImageDigest == ([sha256.Size]byte{}) || rows > math.MaxUint64-relation.Rows {
			return fmt.Errorf("%w: bundle relation %d", ErrSnapshotBase, i+1)
		}
		name := string(relation.Collection)
		if _, exists := seenNames[name]; exists {
			return fmt.Errorf("%w: duplicate bundle relation", ErrSnapshotBase)
		}
		seenNames[name] = struct{}{}
		rows += relation.Rows
	}
	if !bytes.Equal(manifest.UserCollection, manifest.Relations[0].Collection) ||
		rows != manifest.UserRows ||
		canonicalBundleImageDigest(manifest.Relations) != manifest.ImageDigest {
		return fmt.Errorf("%w: bundle image", ErrSnapshotBase)
	}
	stateEnvelope, err := AppendState(nil, manifest.State)
	if err != nil || manifest.Digest != bundleSnapshotManifestDigest(
		stateEnvelope, manifest.RelationManifestDigest, manifest.SystemRows,
		manifest.Relations, manifest.ImageDigest,
	) {
		return fmt.Errorf("%w: bundle identity", ErrSnapshotBase)
	}
	return nil
}

func canonicalBundleImageDigest(relations []SnapshotArtifactRelation) [sha256.Size]byte {
	if len(relations) == 1 {
		return relations[0].ImageDigest
	}
	h := sha256.New()
	_, _ = h.Write(relationImageDigestDomain)
	var fixed [10]byte
	binary.LittleEndian.PutUint64(fixed[:8], uint64(len(relations)))
	_, _ = h.Write(fixed[:8])
	for i := range relations {
		binary.LittleEndian.PutUint16(fixed[8:10], uint16(relations[i].Relation))
		_, _ = h.Write(fixed[8:10])
		_, _ = h.Write(relations[i].ImageDigest[:])
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func bundleSnapshotManifestDigest(
	stateEnvelope []byte,
	manifestDigest [sha256.Size]byte,
	systemRows uint64,
	relations []SnapshotArtifactRelation,
	imageDigest [sha256.Size]byte,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(seededSnapshotManifestDomain)
	writeHashFrame(h, stateEnvelope)
	_, _ = h.Write(manifestDigest[:])
	var fixed [16]byte
	binary.LittleEndian.PutUint64(fixed[0:8], systemRows)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(len(relations)))
	_, _ = h.Write(fixed[:])
	for i := range relations {
		binary.LittleEndian.PutUint16(fixed[0:2], uint16(relations[i].Relation))
		fixed[2] = byte(relations[i].Kind)
		clear(fixed[3:8])
		binary.LittleEndian.PutUint64(fixed[8:16], relations[i].Rows)
		_, _ = h.Write(fixed[:])
		writeHashFrame(h, relations[i].Collection)
		_, _ = h.Write(relations[i].ImageDigest[:])
	}
	_, _ = h.Write(imageDigest[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func seededSnapshotManifestDigest(
	stateEnvelope, userCollection []byte,
	imageDigest [sha256.Size]byte,
	userRows uint64,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(seededSnapshotManifestDomain)
	writeHashFrame(h, stateEnvelope)
	writeHashFrame(h, userCollection)
	_, _ = h.Write(imageDigest[:])
	var rows [8]byte
	binary.LittleEndian.PutUint64(rows[:], userRows)
	_, _ = h.Write(rows[:])
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
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
