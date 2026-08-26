package durable

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCheckpointMembershipCertificateCanonicalRoundTrip(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 1)
	authorization := sha256.Sum256([]byte("schema-rollout-authority"))
	witness, err := group.PrepareMembershipTransition(members, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if witness.Sequence == 0 || witness.Source == ([sha256.Size]byte{}) ||
		witness.Target == ([sha256.Size]byte{}) {
		t.Fatalf("invalid witness: %+v", witness)
	}
	record, err := openCheckpointMembershipCertificate(group.log)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCheckpointMembershipCertificate(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCheckpointMembershipCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := encodeCheckpointMembershipCertificate(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(encoded, reencoded) {
		t.Fatal("decode/re-encode changed canonical bytes")
	}
}

func TestCheckpointMembershipPrepareIsDeviceSilentOnExactRetry(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 128)
	checkpointGroupPut(t, group, 1, members, "one")
	authorization := sha256.Sum256([]byte("catalog-generation-7"))
	first, err := group.PrepareMembershipTransition(members, authorization)
	if err != nil {
		t.Fatal(err)
	}
	markerEpoch := group.markerEpoch
	sequence := group.sequence
	stats := group.Stats()
	second, err := group.PrepareMembershipTransition(members, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("retry witness changed: %+v != %+v", second, first)
	}
	if group.markerEpoch != markerEpoch || group.sequence != sequence || group.Stats() != stats {
		t.Fatal("exact retry performed durability work")
	}
	if group.log.marker.Cursor() != 0 {
		t.Fatalf("prepared marker cursor = %d", group.log.marker.Cursor())
	}
	if err := group.ObserveMembershipTransition(first, authorization); err != nil {
		t.Fatalf("observe exact prepare: %v", err)
	}
	wrong := authorization
	wrong[0] ^= 0xff
	if err := group.ObserveMembershipTransition(first, wrong); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatalf("observe wrong authorization = %v", err)
	}
}

func TestCheckpointMembershipReopenFallsBackFromTornNewestSlot(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 1)
	firstAuth := sha256.Sum256([]byte("first"))
	first, err := group.PrepareMembershipTransition(members, firstAuth)
	if err != nil {
		t.Fatal(err)
	}
	base, err := openCheckpointMembershipCertificate(group.log)
	if err != nil {
		t.Fatal(err)
	}
	base.authorization = sha256.Sum256([]byte("second"))
	second, err := writeCheckpointMembershipCertificate(group.log, base)
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != first.Sequence+1 {
		t.Fatalf("second sequence = %d, want %d", second.Sequence, first.Sequence+1)
	}
	file, err := os.OpenFile(filepath.Join(dir, checkpointMembershipFilename), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(second.Sequence%2)*checkpointMembershipSlotBytes); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := openCheckpointMembershipCertificate(group.log)
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpointMembershipWitness(recovered); got != first {
		t.Fatalf("recovered witness = %+v, want %+v", got, first)
	}
}

func TestCheckpointMembershipPreparedSourceReopensServingOldMembership(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 1)
	checkpointGroupPut(t, group, 1, members, "one")
	authorization := sha256.Sum256([]byte("catalog-not-published"))
	witness, err := group.PrepareMembershipTransition(members, authorization)
	if err != nil {
		t.Fatal(err)
	}
	crashImage := copyCheckpointGroupDirectory(t, dir)
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	collections, log, reopened, err := OpenCollectionsWithCheckpointGroup(
		crashImage, TxnLogOptions{}, requests, []string{"system", "user"},
		CheckpointGroupOptions{},
	)
	for _, file := range files {
		_ = file.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = reopened.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
	}()
	if err := reopened.ObserveMembershipTransition(witness, authorization); err != nil {
		t.Fatalf("observe after crash reopen: %v", err)
	}
}

func TestCheckpointMembershipRejectsAuthenticatedNoncanonicalNewest(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 1)
	authorization := sha256.Sum256([]byte("first"))
	first, err := group.PrepareMembershipTransition(members, authorization)
	if err != nil {
		t.Fatal(err)
	}
	base, err := openCheckpointMembershipCertificate(group.log)
	if err != nil {
		t.Fatal(err)
	}
	base.authorization = sha256.Sum256([]byte("second"))
	second, err := writeCheckpointMembershipCertificate(group.log, base)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, checkpointMembershipFilename)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, checkpointMembershipSlotBytes)
	offset := int64(second.Sequence%2) * checkpointMembershipSlotBytes
	if _, err := file.ReadAt(buf, offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	buf[14] = 1 // authenticated reserved byte
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipDigestDomain)
	_, _ = h.Write(buf[:checkpointMembershipChecksumOffset])
	copy(buf[checkpointMembershipChecksumOffset:], h.Sum(nil))
	if _, err := file.WriteAt(buf, offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openCheckpointMembershipCertificate(group.log); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatalf("authenticated noncanonical newest = %v", err)
	}
	_ = first
}
