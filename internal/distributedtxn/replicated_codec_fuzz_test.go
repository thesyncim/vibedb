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
