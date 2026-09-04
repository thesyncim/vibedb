package distributedtxn

import "testing"

var (
	replicatedSequenceSink ManifestSegmentSequence
	replicatedCommandSink  ReplicatedCommandView
)

func BenchmarkOpenManifestSegmentSequence(b *testing.B) {
	_, pages := buildManifest(b, 17_000)
	for _, test := range []struct {
		name  string
		pages int
	}{
		{name: "one-page", pages: 1},
		{name: "fifteen-pages", pages: MaxManifestSegmentsPerCommand},
	} {
		raw := appendManifestPages(nil, pages[:test.pages])
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				var err error
				replicatedSequenceSink, err = OpenManifestSegmentSequence(raw)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkValidateFusedManifestCoordinator(b *testing.B) {
	descriptor, pages := buildManifest(b, 17_000)
	coordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		b.Fatal(err)
	}
	ordinal := uint32(64)
	command, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role:      ReplicatedRoleCoordinator,
		Operation: ReplicatedBeginPrepareManifestCoordinator,
		ID:        testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
		Payload: appendManifestPages(coordinator, pages[:MaxManifestSegmentsPerCommand]),
		Target: fusedTargetStage(
			ReplicatedBeginPrepareManifestCoordinator, ordinal,
			manifestTarget(uint64(ordinal)).MutationDigest,
		),
	})
	if err != nil {
		b.Fatal(err)
	}
	var scopes [MaxIntentScopes]IntentScope
	b.ReportAllocs()
	b.SetBytes(int64(len(command)))
	for b.Loop() {
		replicatedCommandSink, err = OpenReplicatedCommandInto(command, scopes[:])
		if err != nil {
			b.Fatal(err)
		}
	}
}
