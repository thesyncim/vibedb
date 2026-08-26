package distributedtxn

import (
	"bytes"
	"testing"
)

func FuzzOpenReplicatedCommand(f *testing.F) {
	participant, err := AppendReplicatedCommand(nil, replicatedTestParticipant())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(participant)
	inline, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator,
		Payload: replicatedTestCoordinator(f),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(inline)
	descriptor, pages := buildManifest(f, 4)
	manifestCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		f.Fatal(err)
	}
	manifestStart, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
		Payload: append(manifestCoordinator, pages[0]...),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(manifestStart)
	wideDescriptor, widePages := buildManifest(f, 4097)
	wideCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: wideDescriptor,
	})
	if err != nil {
		f.Fatal(err)
	}
	wideCount := min(len(widePages), MaxManifestSegmentsPerCommand)
	widePayload := appendManifestPages(wideCoordinator, widePages[:wideCount])
	wideOrdinal := uint32(64)
	wideMutation := manifestParticipant(uint64(wideOrdinal)).MutationDigest
	fused, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role:      ReplicatedRoleCoordinator,
		Operation: ReplicatedBeginPrepareManifestCoordinator,
		ID:        testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
		Payload: widePayload,
		Participant: fusedParticipantStage(
			ReplicatedBeginPrepareManifestCoordinator, wideOrdinal, wideMutation,
		),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fused)
	f.Fuzz(func(t *testing.T, raw []byte) {
		view, openErr := OpenReplicatedCommand(raw)
		validateErr := ValidateReplicatedCommand(raw)
		if (openErr == nil) != (validateErr == nil) {
			t.Fatalf("validator/open disagreement: validate=%v open=%v", validateErr, openErr)
		}
		if openErr != nil {
			return
		}
		if !bytes.Equal(view.Bytes(), raw) {
			t.Fatal("accepted view did not retain exact input")
		}
		reencoded, appendErr := AppendReplicatedCommand(nil, view.Command())
		if appendErr != nil {
			t.Fatalf("accepted input did not re-encode: %v", appendErr)
		}
		if !bytes.Equal(reencoded, raw) {
			t.Fatal("accepted input has a second encoding")
		}
	})
}

func FuzzOpenManifestSegmentSequence(f *testing.F) {
	_, pages := buildManifest(f, 4097)
	seed := appendManifestPages(nil, pages)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, raw []byte) {
		sequence, err := OpenManifestSegmentSequence(raw)
		if err != nil {
			return
		}
		if !bytes.Equal(sequence.Bytes(), raw) || sequence.Count() == 0 ||
			sequence.Count() > MaxManifestSegmentsPerCommand {
			t.Fatal("accepted sequence did not borrow one bounded canonical input")
		}
		var rebuilt []byte
		iterator := sequence.Iterator()
		for iterator.Next() {
			page := iterator.Segment()
			if !canonicalManifestSegment(page.Raw) {
				t.Fatal("accepted sequence contains a noncanonical page")
			}
			rebuilt = append(rebuilt, page.Raw...)
		}
		if !bytes.Equal(rebuilt, raw) {
			t.Fatal("sequence iteration is not byte-unique")
		}
	})
}
