package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func TestTransactionManifestCreationRecoveryAfterNonfusedAppend(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	var pages [][]byte
	builder, err := distributedtxn.NewManifestBuilder(make([]byte, distributedtxn.ManifestSegmentBytes),
		func(segment distributedtxn.ManifestSegment) error {
			pages = append(pages, bytes.Clone(segment.Raw))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; len(pages) == 0 && index <= distributedtxn.MaxManifestPageParticipants; index++ {
		if err := builder.Append(distributedtxn.ParticipantRef{
			Distribution: []byte("dist"), Shard: []byte(fmt.Sprintf("shard-%08d", index)),
			RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: transactionCodecDigest(15), State: distributedtxn.ParticipantStaged,
		}); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil || len(pages) != 2 {
		t.Fatalf("two-page fixture: pages=%d err=%v", len(pages), err)
	}
	applied := uint64(2)
	// Two simultaneously retained coordinators also exercise reuse of the one
	// streaming hash across adjacent transaction page ranges.
	for _, identity := range []byte{61, 62} {
		id := transactionCodecID(identity)
		coordinator, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
			ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
			CatalogGeneration: 1, RecoveryDeadline: 2, Manifest: descriptor,
		})
		if err != nil {
			t.Fatal(err)
		}
		stage := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
			Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageManifestCoordinator,
			ID: id, PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
			Payload: appendManifestPageBytes(bytes.Clone(coordinator), pages[:1]),
		}, nil)
		applyTransactionCommand(t, fixture.machine, applied, stage)
		applied++
		assertTransactionManifestSnapshotAndReopen(t, fixture)
		appendPage := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
			Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageManifestSegment,
			ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadManifestSegment,
			Payload: pages[1],
		}, nil)
		applyTransactionCommand(t, fixture.machine, applied, appendPage)
		applied++
		assertTransactionManifestSnapshotAndReopen(t, fixture)
		assertManifestCreationCorruptionRejected(t, fixture, id, coordinator, pages)
	}
}

func assertManifestCreationCorruptionRejected(
	t *testing.T, fixture machineFixture, id distributedtxn.ID, coordinator []byte, pages [][]byte,
) {
	t.Helper()
	controlKey, _ := TransactionControlStorageKey(distributedtxn.ReplicatedRoleCoordinator, id)
	controlRaw, found, err := fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("read control: found=%t err=%v", found, err)
	}
	control, err := OpenTransactionControl(controlRaw)
	if err != nil {
		t.Fatal(err)
	}
	for name, digest := range map[string]distributedtxn.Digest{
		"coordinator-only-digest":   sha256.Sum256(coordinator),
		"arbitrary-creation-digest": transactionCodecDigest(99),
	} {
		altered := control.TransactionControl
		altered.PayloadDigest = digest
		encoded, err := AppendTransactionControl(nil, altered)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			assertManifestRowCorruptionRejected(t, fixture, controlKey[:], encoded)
		})
	}
	initialPages := 1
	if control.FusedPath {
		initialPages = min(len(pages), distributedtxn.MaxManifestSegmentsPerCommand)
	}
	if initialPages != len(pages) || initialPages > 1 {
		wrongPrefix := len(pages)
		if initialPages == len(pages) {
			wrongPrefix--
		}
		altered := control.TransactionControl
		altered.PayloadDigest = sha256.Sum256(appendManifestPageBytes(bytes.Clone(coordinator), pages[:wrongPrefix]))
		encoded, err := AppendTransactionControl(nil, altered)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("wrong-initial-prefix-count", func(t *testing.T) {
			assertManifestRowCorruptionRejected(t, fixture, controlKey[:], encoded)
		})
	}
	t.Run("creation-seed", func(t *testing.T) {
		altered := control.TransactionControl
		altered.MutationDigest[0] ^= 1
		encoded, err := AppendTransactionControl(nil, altered)
		if err != nil {
			t.Fatal(err)
		}
		assertManifestRowCorruptionRejected(t, fixture, controlKey[:], encoded)
	})
	t.Run("canonical-coordinator-substitution", func(t *testing.T) {
		record, err := distributedtxn.OpenManifestCoordinator(coordinator)
		if err != nil {
			t.Fatal(err)
		}
		record.RecoveryDeadline++
		altered, err := distributedtxn.AppendManifestCoordinator(nil, record)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := AppendTransactionCoordinatorPayload(nil, id, distributedtxn.ReplicatedPayloadManifestCoordinator, altered)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := TransactionCoordinatorPayloadStorageKey(id)
		assertManifestRowCorruptionRejected(t, fixture, key[:], encoded)
	})
	t.Run("canonical-first-page-substitution", func(t *testing.T) {
		page, err := distributedtxn.OpenManifestSegment(pages[0],
			make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants),
			make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2))
		if err != nil {
			t.Fatal(err)
		}
		page.Participants[0].MutationDigest[0] ^= 0x80
		var encoded []byte
		builder, err := distributedtxn.NewManifestBuilder(make([]byte, distributedtxn.ManifestSegmentBytes),
			func(segment distributedtxn.ManifestSegment) error {
				var encodeErr error
				encoded, encodeErr = AppendTransactionManifestPage(nil, id, segment)
				return encodeErr
			})
		if err != nil {
			t.Fatal(err)
		}
		for _, participant := range page.Participants {
			if err := builder.Append(participant); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := builder.Seal(); err != nil {
			t.Fatal(err)
		}
		key, _ := TransactionManifestPageStorageKey(id, 0)
		assertManifestRowCorruptionRejected(t, fixture, key[:], encoded)
	})
}

func assertManifestRowCorruptionRejected(t *testing.T, fixture machineFixture, key, altered []byte) {
	t.Helper()
	original, found, err := fixture.system.Collection.AppendRaw(nil, key)
	if err != nil || !found || len(original) != len(altered) {
		t.Fatalf("substitution must preserve accounting: found=%t bytes=%d/%d err=%v", found, len(original), len(altered), err)
	}
	if _, err := fixture.system.Collection.Put(key, altered); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := fixture.system.Collection.Put(key, original); err != nil {
			t.Error(err)
		}
	})
	if _, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options); !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("corrupt manifest creation reopened: %v", err)
	}
}
