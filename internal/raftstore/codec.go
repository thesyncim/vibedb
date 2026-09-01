package raftstore

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"slices"
	"unicode/utf8"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const codecVersion uint16 = 2

type decoder struct {
	data   []byte
	offset int
}

func (d *decoder) take(size int) ([]byte, error) {
	if size < 0 || d.offset > len(d.data)-size {
		return nil, fmt.Errorf("%w: truncated codec payload", ErrCorrupt)
	}
	result := d.data[d.offset : d.offset+size]
	d.offset += size
	return result, nil
}

func (d *decoder) u8() (uint8, error) {
	value, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (d *decoder) u16() (uint16, error) {
	value, err := d.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (d *decoder) u32() (uint32, error) {
	value, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (d *decoder) u64() (uint64, error) {
	value, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (d *decoder) done() error {
	if d.offset != len(d.data) {
		return fmt.Errorf("%w: %d trailing codec bytes", ErrCorrupt, len(d.data)-d.offset)
	}
	return nil
}

func appendUint16(dst []byte, value uint16) []byte {
	return binary.LittleEndian.AppendUint16(dst, value)
}
func appendUint32(dst []byte, value uint32) []byte {
	return binary.LittleEndian.AppendUint32(dst, value)
}
func appendUint64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}

type fileCrypto struct {
	dataKey  [32]byte
	aead     cipher.AEAD
	nonceKey [32]byte
}

// objectCryptoWorkspace owns keyed HMAC state for one single-threaded record
// scanner. Reset preserves each HMAC's key schedule, while sequence and sum
// storage stay caller-owned. It must never be shared between goroutines.
type objectCryptoWorkspace struct {
	objectKeyMAC hash.Hash
	authMAC      hash.Hash
	sequence     [8]byte
	domain       [32]byte
	digest       [sha256.Size]byte
	sum          [sha256.Size]byte
}

var (
	objectKeyPrefix   = []byte("vibedb/raft-wal/object/")
	objectTagPrefix   = []byte("tag/")
	objectNoncePrefix = []byte("object/")
)

func (workspace *objectCryptoWorkspace) writeDomain(mac hash.Hash, domain string) {
	if len(domain) > len(workspace.domain) {
		panic("raftstore: object crypto domain exceeds workspace")
	}
	copy(workspace.domain[:], domain)
	_, _ = mac.Write(workspace.domain[:len(domain)])
}

func newObjectCryptoWorkspace(
	dataKey [sha256.Size]byte,
	nonceKey [sha256.Size]byte,
) objectCryptoWorkspace {
	return objectCryptoWorkspace{
		objectKeyMAC: hmac.New(sha256.New, dataKey[:]),
		authMAC:      hmac.New(sha256.New, nonceKey[:]),
	}
}

func (workspace *objectCryptoWorkspace) deriveObjectKey(
	domain string,
	sequence uint64,
	digest [sha256.Size]byte,
) [sha256.Size]byte {
	mac := workspace.objectKeyMAC
	mac.Reset()
	_, _ = mac.Write(objectKeyPrefix)
	workspace.writeDomain(mac, domain)
	binary.LittleEndian.PutUint64(workspace.sequence[:], sequence)
	_, _ = mac.Write(workspace.sequence[:])
	workspace.digest = digest
	_, _ = mac.Write(workspace.digest[:])
	_ = mac.Sum(workspace.sum[:0])
	return workspace.sum
}

func (workspace *objectCryptoWorkspace) makeObjectTag(
	domain string,
	sequence uint64,
	context, payload []byte,
) [sha256.Size]byte {
	mac := workspace.authMAC
	mac.Reset()
	_, _ = mac.Write(objectTagPrefix)
	workspace.writeDomain(mac, domain)
	binary.LittleEndian.PutUint64(workspace.sequence[:], sequence)
	_, _ = mac.Write(workspace.sequence[:])
	_, _ = mac.Write(context)
	_, _ = mac.Write(payload)
	_ = mac.Sum(workspace.sum[:0])
	return workspace.sum
}

func (workspace *objectCryptoWorkspace) deriveObjectNonce(
	domain string,
	sequence uint64,
	digest [sha256.Size]byte,
) [12]byte {
	mac := workspace.authMAC
	mac.Reset()
	_, _ = mac.Write(objectNoncePrefix)
	workspace.writeDomain(mac, domain)
	binary.LittleEndian.PutUint64(workspace.sequence[:], sequence)
	_, _ = mac.Write(workspace.sequence[:])
	workspace.digest = digest
	_, _ = mac.Write(workspace.digest[:])
	_ = mac.Sum(workspace.sum[:0])
	var result [12]byte
	copy(result[:], workspace.sum[:])
	return result
}

func makeFileCrypto(key Key, fileID [16]byte) (fileCrypto, error) {
	if err := validateKey(key, false); err != nil {
		return fileCrypto{}, err
	}
	if fileID == ([16]byte{}) {
		return fileCrypto{}, fmt.Errorf("%w: zero file ID", ErrInvalid)
	}
	dataKey := deriveFileSecret(key.Material, fileID, "aes-256-gcm-data-key")
	nonceKey := deriveFileSecret(key.Material, fileID, "nonce-key")
	block, err := aes.NewCipher(dataKey[:])
	if err != nil {
		return fileCrypto{}, fmt.Errorf("%w: initialize AES: %v", ErrInvalid, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return fileCrypto{}, fmt.Errorf("%w: initialize GCM: %v", ErrInvalid, err)
	}
	if aead.NonceSize() != 12 || aead.Overhead() != 16 {
		return fileCrypto{}, fmt.Errorf("%w: unsupported GCM geometry", ErrInvalid)
	}
	return fileCrypto{dataKey: dataKey, aead: aead, nonceKey: nonceKey}, nil
}

func makeObjectAEAD(dataKey [32]byte, domain string, sequence uint64, digest [32]byte) (cipher.AEAD, error) {
	derived := deriveObjectKey(dataKey, domain, sequence, digest)
	return makeObjectAEADFromKey(derived)
}

func makeObjectAEADFromKey(derived [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func deriveObjectKey(dataKey [32]byte, domain string, sequence uint64, digest [32]byte) [32]byte {
	mac := hmac.New(sha256.New, dataKey[:])
	_, _ = mac.Write([]byte("vibedb/raft-wal/object/"))
	_, _ = mac.Write([]byte(domain))
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], sequence)
	_, _ = mac.Write(encoded[:])
	_, _ = mac.Write(digest[:])
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func makeObjectTag(key [32]byte, domain string, sequence uint64, context, payload []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("tag/"))
	_, _ = mac.Write([]byte(domain))
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], sequence)
	_, _ = mac.Write(encoded[:])
	_, _ = mac.Write(context)
	_, _ = mac.Write(payload)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func deriveObjectNonce(key [32]byte, domain string, sequence uint64, digest [32]byte) [12]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("object/"))
	_, _ = mac.Write([]byte(domain))
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], sequence)
	_, _ = mac.Write(encoded[:])
	_, _ = mac.Write(digest[:])
	var result [12]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func deriveFileSecret(material [32]byte, fileID [16]byte, domain string) [32]byte {
	mac := hmac.New(sha256.New, material[:])
	_, _ = mac.Write([]byte("vibedb/raft-wal/"))
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write(fileID[:])
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func deriveNonce(key [32]byte, domain string, sequence uint64) [12]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte(domain))
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], sequence)
	_, _ = mac.Write(encoded[:])
	var result [12]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func validateKey(key Key, creating bool) error {
	if len(key.ID) == 0 || len(key.ID) > MaxKeyIDBytes || !utf8.ValidString(key.ID) || bytes.IndexByte([]byte(key.ID), 0) >= 0 {
		return fmt.Errorf("%w: invalid key ID", ErrInvalid)
	}
	var nonzero byte
	for _, value := range key.Material {
		nonzero |= value
	}
	if nonzero == 0 {
		return fmt.Errorf("%w: zero AES-256 key", ErrInvalid)
	}
	if len(key.Wrapped) > MaxWrappedKeyBytes || (creating && len(key.Wrapped) == 0) {
		return fmt.Errorf("%w: invalid wrapped-key metadata length", ErrInvalid)
	}
	return nil
}

func validateIdentity(identity Identity) error {
	if len(identity.Distribution) == 0 || len(identity.Distribution) > MaxIdentityComponentBytes ||
		len(identity.Shard) == 0 || len(identity.Shard) > MaxIdentityComponentBytes ||
		!utf8.ValidString(identity.Distribution) || !utf8.ValidString(identity.Shard) ||
		bytes.IndexByte([]byte(identity.Distribution), 0) >= 0 || bytes.IndexByte([]byte(identity.Shard), 0) >= 0 ||
		identity.AllocationGeneration == 0 ||
		identity.MemberID == raft.None || raft.IsLocalMsgTarget(identity.MemberID) {
		return fmt.Errorf("%w: invalid immutable identity", ErrInvalid)
	}
	for name, value := range map[string][16]byte{
		"cluster ID":          identity.ClusterID,
		"cluster incarnation": identity.ClusterIncarnation,
		"shard incarnation":   identity.ShardIncarnation,
		"group ID":            identity.GroupID,
		"store ID":            identity.StoreID,
	} {
		if value == ([16]byte{}) {
			return fmt.Errorf("%w: zero %s", ErrInvalid, name)
		}
	}
	return nil
}

// ValidateIdentity validates the immutable member identity without creating,
// opening, allocating, or otherwise mutating a WAL namespace.
func ValidateIdentity(identity Identity) error {
	return validateIdentity(identity)
}

func readFresh(reader io.Reader, destination []byte) error {
	if _, err := io.ReadFull(reader, destination); err != nil {
		return fmt.Errorf("%w: random bytes: %v", ErrInvalid, err)
	}
	var nonzero byte
	for _, value := range destination {
		nonzero |= value
	}
	if nonzero == 0 {
		return fmt.Errorf("%w: random source returned all zeroes", ErrInvalid)
	}
	return nil
}

func marshalSnapshot(snapshot *pb.Snapshot) ([]byte, error) {
	if err := validateSnapshotBase(snapshot, 0); err != nil {
		return nil, err
	}
	metadata := snapshot.GetMetadata()
	conf := metadata.GetConfState()
	capacity := 40 + 8*(len(conf.GetVoters())+len(conf.GetLearners())) + len(snapshot.GetData())
	result := make([]byte, 0, capacity)
	result = appendUint16(result, codecVersion)
	flags := uint16(0)
	if conf.AutoLeave != nil {
		flags = 1
	}
	result = appendUint16(result, flags)
	result = appendUint64(result, metadata.GetIndex())
	result = appendUint64(result, metadata.GetTerm())
	result = appendUint16(result, uint16(len(conf.GetVoters())))
	result = appendUint16(result, uint16(len(conf.GetLearners())))
	result = appendUint16(result, 0)
	result = appendUint16(result, 0)
	result = appendUint32(result, uint32(len(snapshot.GetData())))
	result = appendUint32(result, 0)
	for _, id := range conf.GetVoters() {
		result = appendUint64(result, id)
	}
	for _, id := range conf.GetLearners() {
		result = appendUint64(result, id)
	}
	result = append(result, snapshot.GetData()...)
	return result, nil
}

func unmarshalSnapshot(data []byte, memberID uint64) (*pb.Snapshot, error) {
	reader := decoder{data: data}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return nil, fmt.Errorf("%w: snapshot codec version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags&^uint16(1) != 0 {
		return nil, fmt.Errorf("%w: snapshot flags", ErrCorrupt)
	}
	index, err := reader.u64()
	if err != nil {
		return nil, err
	}
	term, err := reader.u64()
	if err != nil {
		return nil, err
	}
	voters, err := reader.u16()
	if err != nil {
		return nil, err
	}
	learners, err := reader.u16()
	if err != nil {
		return nil, err
	}
	outgoing, err := reader.u16()
	if err != nil {
		return nil, err
	}
	next, err := reader.u16()
	if err != nil {
		return nil, err
	}
	dataLength, err := reader.u32()
	if err != nil {
		return nil, err
	}
	reserved, err := reader.u32()
	if err != nil {
		return nil, err
	}
	if outgoing != 0 || next != 0 || reserved != 0 || int(voters)+int(learners) > MaxBootstrapMembers || dataLength > MaxSnapshotBaseBytes {
		return nil, fmt.Errorf("%w: snapshot geometry", ErrCorrupt)
	}
	conf := &pb.ConfState{Voters: make([]uint64, int(voters)), Learners: make([]uint64, int(learners))}
	if flags&1 != 0 {
		conf.AutoLeave = boolPointer(false)
	}
	for position := range conf.Voters {
		conf.Voters[position], err = reader.u64()
		if err != nil {
			return nil, err
		}
	}
	for position := range conf.Learners {
		conf.Learners[position], err = reader.u64()
		if err != nil {
			return nil, err
		}
	}
	payload, err := reader.take(int(dataLength))
	if err != nil {
		return nil, err
	}
	if err := reader.done(); err != nil {
		return nil, err
	}
	snapshot := &pb.Snapshot{Data: append([]byte(nil), payload...), Metadata: &pb.SnapshotMetadata{
		ConfState: conf, Index: uint64Pointer(index), Term: uint64Pointer(term),
	}}
	if err := validateSnapshotBase(snapshot, memberID); err != nil {
		return nil, fmt.Errorf("%w: decoded snapshot: %v", ErrCorrupt, err)
	}
	return snapshot, nil
}

func validateSnapshotBase(snapshot *pb.Snapshot, memberID uint64) error {
	if snapshot == nil || snapshot.GetMetadata() == nil || snapshot.GetMetadata().GetConfState() == nil ||
		len(snapshot.ProtoReflect().GetUnknown()) != 0 || len(snapshot.GetMetadata().ProtoReflect().GetUnknown()) != 0 ||
		len(snapshot.GetMetadata().GetConfState().ProtoReflect().GetUnknown()) != 0 ||
		snapshot.GetMetadata().GetIndex() == 0 || snapshot.GetMetadata().GetIndex() == math.MaxUint64 ||
		snapshot.GetMetadata().GetTerm() == 0 || snapshot.GetMetadata().GetTerm() == math.MaxUint64 ||
		len(snapshot.GetData()) > MaxSnapshotBaseBytes {
		return fmt.Errorf("%w: snapshot base metadata or bytes", ErrInvalid)
	}
	conf := snapshot.GetMetadata().GetConfState()
	if len(conf.GetVoters()) == 0 || len(conf.GetVoters())+len(conf.GetLearners()) > MaxBootstrapMembers ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return fmt.Errorf("%w: snapshot-base ConfState must be stable", ErrInvalid)
	}
	all := make([]uint64, 0, len(conf.GetVoters())+len(conf.GetLearners()))
	for _, list := range [][]uint64{conf.GetVoters(), conf.GetLearners()} {
		if !slices.IsSorted(list) {
			return fmt.Errorf("%w: bootstrap member IDs are not canonical", ErrInvalid)
		}
		var previous uint64
		for index, id := range list {
			if id == raft.None || raft.IsLocalMsgTarget(id) || (index != 0 && id == previous) {
				return fmt.Errorf("%w: invalid snapshot-base member ID", ErrInvalid)
			}
			previous = id
			all = append(all, id)
		}
	}
	slices.Sort(all)
	for index := 1; index < len(all); index++ {
		if all[index] == all[index-1] {
			return fmt.Errorf("%w: snapshot-base member appears twice", ErrInvalid)
		}
	}
	if memberID != 0 && !slices.Contains(all, memberID) {
		return fmt.Errorf("%w: local member is absent from snapshot-base ConfState", ErrInvalid)
	}
	return nil
}

type snapshotReference struct {
	id     [16]byte
	digest [32]byte
	size   uint64
	index  uint64
	term   uint64
}

type formatBounds struct {
	fileBytes   uint64
	recordBytes uint32
	records     uint64
	entries     uint64
	liveBytes   uint64
}

func boundsFromOptions(options normalizedOptions) formatBounds {
	return formatBounds{fileBytes: uint64(options.maxFileBytes), recordBytes: uint32(options.maxRecordBytes), records: options.maxRecords, entries: options.maxEntries, liveBytes: uint64(options.maxLiveBytes)}
}

func marshalBootstrap(bootstrap Bootstrap, memberID uint64) ([]byte, []byte, error) {
	if bootstrap.TopologyRecoveryEpoch == 0 {
		return nil, nil, fmt.Errorf("%w: zero topology recovery epoch", ErrInvalid)
	}
	if err := validateSnapshotBase(bootstrap.Snapshot, memberID); err != nil {
		return nil, nil, err
	}
	snapshotBytes, err := marshalSnapshot(bootstrap.Snapshot)
	if err != nil {
		return nil, nil, err
	}
	result := make([]byte, 0, 16+len(snapshotBytes))
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, 0)
	result = appendUint64(result, bootstrap.TopologyRecoveryEpoch)
	result = appendUint32(result, uint32(len(snapshotBytes)))
	result = appendUint32(result, 0)
	result = append(result, snapshotBytes...)
	return result, snapshotBytes, nil
}

func unmarshalBootstrap(data []byte, memberID uint64) (Bootstrap, []byte, error) {
	reader := decoder{data: data}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return Bootstrap{}, nil, fmt.Errorf("%w: bootstrap codec version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags != 0 {
		return Bootstrap{}, nil, fmt.Errorf("%w: bootstrap flags", ErrCorrupt)
	}
	epoch, err := reader.u64()
	if err != nil || epoch == 0 {
		return Bootstrap{}, nil, fmt.Errorf("%w: bootstrap epoch", ErrCorrupt)
	}
	snapshotLength, err := reader.u32()
	if err != nil || snapshotLength > MaxSnapshotBaseBytes+1024 {
		return Bootstrap{}, nil, fmt.Errorf("%w: bootstrap length", ErrCorrupt)
	}
	reserved, err := reader.u32()
	if err != nil || reserved != 0 {
		return Bootstrap{}, nil, fmt.Errorf("%w: bootstrap reserved", ErrCorrupt)
	}
	snapshotBytes, err := reader.take(int(snapshotLength))
	if err != nil {
		return Bootstrap{}, nil, err
	}
	if err := reader.done(); err != nil {
		return Bootstrap{}, nil, err
	}
	snapshot, err := unmarshalSnapshot(snapshotBytes, memberID)
	if err != nil {
		return Bootstrap{}, nil, err
	}
	return Bootstrap{TopologyRecoveryEpoch: epoch, Snapshot: snapshot}, slices.Clone(snapshotBytes), nil
}

func marshalIdentity(identity Identity, reference snapshotReference, bounds formatBounds) ([]byte, error) {
	if err := validateIdentity(identity); err != nil {
		return nil, err
	}
	if reference.id == ([16]byte{}) || reference.size == 0 || reference.index == 0 || reference.term == 0 {
		return nil, fmt.Errorf("%w: invalid bootstrap reference", ErrInvalid)
	}
	result := make([]byte, 0, 192+len(identity.Distribution)+len(identity.Shard))
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, uint16(len(identity.Distribution)))
	result = appendUint16(result, uint16(len(identity.Shard)))
	result = appendUint16(result, 0)
	result = append(result, identity.ClusterID[:]...)
	result = append(result, identity.ClusterIncarnation[:]...)
	result = appendUint64(result, identity.AllocationGeneration)
	result = append(result, identity.ShardIncarnation[:]...)
	result = append(result, identity.GroupID[:]...)
	result = appendUint64(result, identity.MemberID)
	result = append(result, identity.StoreID[:]...)
	result = append(result, reference.id[:]...)
	result = append(result, reference.digest[:]...)
	result = appendUint64(result, reference.size)
	result = appendUint64(result, reference.index)
	result = appendUint64(result, reference.term)
	result = appendUint64(result, bounds.fileBytes)
	result = appendUint32(result, bounds.recordBytes)
	result = appendUint32(result, 0)
	result = appendUint64(result, bounds.records)
	result = appendUint64(result, bounds.entries)
	result = appendUint64(result, bounds.liveBytes)
	result = append(result, identity.Distribution...)
	result = append(result, identity.Shard...)
	return result, nil
}

func unmarshalIdentity(data []byte) (Identity, snapshotReference, formatBounds, error) {
	reader := decoder{data: data}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: identity codec version", ErrCorrupt)
	}
	distributionLength, err := reader.u16()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	shardLength, err := reader.u16()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	reserved, err := reader.u16()
	if err != nil || reserved != 0 || distributionLength == 0 || distributionLength > MaxIdentityComponentBytes || shardLength == 0 || shardLength > MaxIdentityComponentBytes {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: identity string geometry", ErrCorrupt)
	}
	var identity Identity
	value, err := reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(identity.ClusterID[:], value)
	value, err = reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(identity.ClusterIncarnation[:], value)
	identity.AllocationGeneration, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	value, err = reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(identity.ShardIncarnation[:], value)
	value, err = reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(identity.GroupID[:], value)
	identity.MemberID, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	value, err = reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(identity.StoreID[:], value)
	var reference snapshotReference
	value, err = reader.take(16)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(reference.id[:], value)
	value, err = reader.take(32)
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	copy(reference.digest[:], value)
	reference.size, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	reference.index, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	reference.term, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	var bounds formatBounds
	bounds.fileBytes, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	bounds.recordBytes, err = reader.u32()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	reservedBounds, err := reader.u32()
	if err != nil || reservedBounds != 0 {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: bounds reserved", ErrCorrupt)
	}
	bounds.records, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	bounds.entries, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	bounds.liveBytes, err = reader.u64()
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	value, err = reader.take(int(distributionLength))
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	identity.Distribution = string(value)
	value, err = reader.take(int(shardLength))
	if err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	identity.Shard = string(value)
	if err := reader.done(); err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, err
	}
	if err := validateIdentity(identity); err != nil {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: decoded identity: %v", ErrCorrupt, err)
	}
	if reference.id == ([16]byte{}) || reference.size == 0 ||
		reference.size > MaxSnapshotBaseBytes+1024 || reference.index == 0 ||
		reference.index == math.MaxUint64 || reference.term == 0 || reference.term == math.MaxUint64 {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: invalid snapshot-base reference", ErrCorrupt)
	}
	if bounds.fileBytes < HeaderBytes || bounds.fileBytes > uint64(AbsoluteMaxFileBytes) || bounds.recordBytes == 0 || bounds.recordBytes > AbsoluteMaxRecordBytes ||
		bounds.records == 0 || bounds.records > AbsoluteMaxRecords || bounds.entries == 0 || bounds.entries > AbsoluteMaxEntries || bounds.liveBytes == 0 || bounds.liveBytes > uint64(AbsoluteMaxLiveBytes) {
		return Identity{}, snapshotReference{}, formatBounds{}, fmt.Errorf("%w: invalid sealed bounds", ErrCorrupt)
	}
	return identity, reference, bounds, nil
}

func addInt64(left int64, right int) (int64, bool) {
	if right < 0 || left > math.MaxInt64-int64(right) {
		return 0, false
	}
	return left + int64(right), true
}
