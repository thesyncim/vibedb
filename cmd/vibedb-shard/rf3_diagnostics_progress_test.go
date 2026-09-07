package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
)

func TestRF3DiagnosticCanaryCountersMapExactSnapshots(t *testing.T) {
	snapshot := rf3DiagnosticSnapshot{
		ReadyWaveHistogram:              make([]uint64, raftstore.MaxPersistGroupBatches+1),
		ReadySeriesHistogram:            make([]uint64, raftstore.MaxReadySeries+1),
		ReadyDurableHistogram:           make([]uint64, raftstore.MaxReadySeries+1),
		RaftProposalQueueDepthHistogram: make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalEntriesPerReady:     make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalBytesPerReady:       make([]uint64, raftservice.ProposalBytesHistogramBuckets),
	}
	applyRF3DiagnosticProgress(&snapshot, raftservice.ProgressMetricsSnapshot{
		ProposalBatches:                 1,
		ProposalCommands:                2,
		ProposalBytes:                   3,
		ApplyBatches:                    4,
		AppliedEntries:                  5,
		CommitAdvancements:              6,
		CommittedEntries:                7,
		ReadyPersisted:                  8,
		ProposalWindowQueued:            9,
		LateJoinUsed:                    10,
		LateJoinMissed:                  11,
		LateJoinEntries:                 12,
		AuthorityReadHits:               16,
		AuthorityReadIndexFallbacks:     17,
		AuthorityReadValidationRetries:  18,
		AuthorityReadValidationFailures: 19,
		AuthorityRoundAttempts:          20,
		ReadIndexShared:                 21,
		ProposalQueueDepthHistogram:     [raftservice.ProposalEntryHistogramBuckets]uint64{2: 13},
		ProposalEntriesPerReady:         [raftservice.ProposalEntryHistogramBuckets]uint64{2: 14},
		ProposalBytesPerReady:           [raftservice.ProposalBytesHistogramBuckets]uint64{1: 15},
	})
	if snapshot.RaftProposalBatches != 1 || snapshot.RaftProposalCommands != 2 ||
		snapshot.RaftProposalBytes != 3 || snapshot.RaftApplyBatches != 4 ||
		snapshot.RaftAppliedEntries != 5 || snapshot.RaftCommitAdvancements != 6 ||
		snapshot.RaftCommittedEntries != 7 || snapshot.RaftReadyPersisted != 8 ||
		snapshot.RaftProposalWindowQueued != 9 || snapshot.RaftLateJoinUsed != 10 ||
		snapshot.RaftLateJoinMissed != 11 || snapshot.RaftLateJoinEntries != 12 ||
		snapshot.RaftProposalQueueDepthHistogram[2] != 13 ||
		snapshot.RaftProposalEntriesPerReady[2] != 14 ||
		snapshot.RaftProposalBytesPerReady[1] != 15 ||
		snapshot.AuthorityReadHits != 16 ||
		snapshot.AuthorityReadIndexFallbacks != 17 ||
		snapshot.AuthorityReadValidationRetries != 18 ||
		snapshot.AuthorityReadValidationFailures != 19 ||
		snapshot.AuthorityRoundAttempts != 20 || snapshot.ReadIndexShared != 21 {
		t.Fatalf("progress counters were not copied exactly: %+v", snapshot)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal diagnostic snapshot: %v", err)
	}
	var encoded map[string]any
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatalf("decode diagnostic snapshot: %v", err)
	}
	for _, key := range []string{
		"authority_read_hits", "authority_read_index_fallbacks",
		"authority_read_validation_retries", "authority_read_validation_failures",
		"authority_round_attempts", "read_index_shared",
	} {
		if _, ok := encoded[key]; !ok {
			t.Fatalf("diagnostic JSON omitted %q: %s", key, raw)
		}
	}

	applyRF3DiagnosticSequencer(&snapshot, raftstore.NodeSubmissionSequencerStats{
		ReadySubmissions:                11,
		ReadyQueueWaitNanos:             12,
		ReadyWavesAttempted:             13,
		ReadyPersistAttempts:            14,
		ReadyPersistSuccesses:           15,
		ReadyPersistFailures:            16,
		ReadyWavesFailed:                17,
		ReadyPersistDurationNanos:       18,
		ReadyWaveDurationNanos:          19,
		ReadyLogicalBatches:             20,
		ReadySeriesSubmissions:          21,
		ReadySingletonSeriesSubmissions: 22,
		ReadyMultiSeriesSubmissions:     23,
		ReadySeriesHistogram:            [raftstore.MaxReadySeries + 1]uint64{2: 24},
		ReadyDurableLogicalBatches:      25,
		ReadyDurableSeriesSubmissions:   26,
		ReadyDurableSeriesHistogram:     [raftstore.MaxReadySeries + 1]uint64{2: 27},
	})
	if snapshot.ReadySubmissions != 11 || snapshot.ReadyQueueWaitNs != 12 ||
		snapshot.ReadyWavesAttempted != 13 || snapshot.ReadyPersistAttempts != 14 ||
		snapshot.ReadyPersistSuccesses != 15 || snapshot.ReadyPersistFailures != 16 ||
		snapshot.ReadyWavesFailed != 17 || snapshot.ReadyPersistDurationNs != 18 ||
		snapshot.ReadyWaveDurationNs != 19 || snapshot.ReadyLogicalBatches != 20 ||
		snapshot.ReadySeriesSubmissions != 21 || snapshot.ReadySingletonSeries != 22 ||
		snapshot.ReadyMultiSeries != 23 || snapshot.ReadySeriesHistogram[2] != 24 ||
		snapshot.ReadyDurableLogical != 25 || snapshot.ReadyDurableSeries != 26 ||
		snapshot.ReadyDurableHistogram[2] != 27 {
		t.Fatalf("sequencer counters were not copied exactly: %+v", snapshot)
	}

	applyRF3DiagnosticProgress(nil, raftservice.ProgressMetricsSnapshot{})
	applyRF3DiagnosticSequencer(nil, raftstore.NodeSubmissionSequencerStats{})
}

func TestRF3DiagnosticAuthorityEvidencePreservesIdentityAndGateReasons(t *testing.T) {
	var group raftmember.GroupKey
	group.ClusterID[0] = 1
	group.ClusterIncarnation[0] = 2
	group.ShardIncarnation[0] = 3
	group.GroupID[0] = 4
	var store [16]byte
	store[0] = 5
	var relationDigest [32]byte
	relationDigest[0] = 6
	policyDigest := [32]byte{7}
	configDigest := [32]byte{8}
	identity := raftmember.RuntimeIdentity{
		Group: group, Distribution: "d", Shard: "s", AllocationGeneration: 9,
		MemberID: 10, StoreID: store, NodeIncarnation: 11,
		RelationManifestDigest: relationDigest,
	}
	authorityGroup := raftauthority.GroupIdentity{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
	}
	request := raftauthority.AuthorityRequest{
		Group: authorityGroup, Term: 12, Holder: 10, HolderIncarnation: 11,
		Config:        raftauthority.ConfigIdentity{AppliedVersion: 13, Digest: configDigest},
		PolicyVersion: 14, PolicyDigest: policyDigest, Nonce: 15,
		StartAt: 16 * time.Nanosecond,
	}
	var gate raftmember.ReadAuthorityGateMetrics
	gate.BlockedByReason[raftmember.ReadAuthorityGateTick][raftmember.ReadAuthorityGateQuarantine] = 17
	gate.BlockedByReason[raftmember.ReadAuthorityGateTick][raftmember.ReadAuthorityGateClockFault] = 18
	gate.BlockedTime[raftmember.ReadAuthorityGateTick][raftmember.ReadAuthorityGateQuarantine] = raftmember.ReadAuthorityGateBlockTime{
		First: 19 * time.Nanosecond, Last: 20 * time.Nanosecond, Available: true,
	}
	evidence := raftmember.ReadAuthorityEvidence{
		Identity: identity, Status: raftmember.ReadAuthorityEvidenceConfigured,
		PolicyVersion: 14, PolicyDigest: policyDigest,
		Clock: raftauthority.CheckedClockEvidence{Sample: 21 * time.Nanosecond, Initialized: true},
		Observation: raftauthority.AuthorityObservation{
			Group: authorityGroup, Term: 12, Leader: 10, LeaderIncarnation: 11,
			Config: request.Config, CurrentTermCommitted: true, Stable: true,
		},
		ObservationAvailable: true,
		Promise: raftauthority.PromiseBookEvidence{
			Group: authorityGroup, LocalMember: 10, HasRecord: true,
			Request: request, GrantedAt: 22 * time.Nanosecond, PromiseUntil: 23 * time.Nanosecond,
			QuarantineConfigured: true, QuarantineAt: 23 * time.Nanosecond,
			QuarantineUntil: 24 * time.Nanosecond,
		},
		PromiseKnown: true, PromiseActive: true, QuarantineKnown: true,
		QuarantineActive: true,
		Holder: raftmember.ReadAuthorityHolderEvidence{
			Available: true, Request: request, ExpiresAt: 25 * time.Nanosecond,
			AcceptedVoters: []uint64{10, 16},
		},
		Gate: gate,
	}
	encoded := rf3DiagnosticAuthorityGroupEvidence(evidence)
	if encoded.RuntimeIdentity.NodeIncarnation != identity.NodeIncarnation ||
		encoded.RuntimeIdentity.StoreID != "05000000000000000000000000000000" ||
		encoded.RuntimeIdentity.RelationManifestDigest != "0600000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("runtime identity JSON = %+v", encoded.RuntimeIdentity)
	}
	if encoded.PolicyDigest != "0700000000000000000000000000000000000000000000000000000000000000" ||
		encoded.Promise.GrantedAtNs != 22 || encoded.Holder.Request.Nonce != request.Nonce || encoded.Holder.ExpiresAtNs != 25 {
		t.Fatalf("policy/holder JSON = %+v", encoded)
	}
	if len(encoded.Gate.Inputs) != raftmember.ReadAuthorityGateInputCount ||
		encoded.Gate.Inputs[raftmember.ReadAuthorityGateTick].Input != "tick" {
		t.Fatalf("gate inputs = %+v", encoded.Gate.Inputs)
	}
	tick := encoded.Gate.Inputs[raftmember.ReadAuthorityGateTick]
	if tick.BlockedByQuarantine != 17 || tick.BlockedByClockFault != 18 ||
		!tick.QuarantineBlockedTime.Available || tick.QuarantineBlockedTime.FirstNs != 19 ||
		tick.QuarantineBlockedTime.LastNs != 20 {
		t.Fatalf("gate reason evidence = %+v", tick)
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal authority evidence: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode authority evidence: %v", err)
	}
	for _, key := range []string{"runtime_identity", "policy_digest", "promise", "holder", "gate"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("authority evidence JSON omitted %q: %s", key, raw)
		}
	}
	groupTwo := group
	groupTwo.GroupID[0] = 9
	groupTwoEvidence := evidence
	groupTwoEvidence.Identity.Group = groupTwo
	expected := map[raftmember.GroupKey]struct{}{group: {}, groupTwo: {}}
	if !rf3DiagnosticAuthorityEvidenceCovers([]raftmember.ReadAuthorityEvidence{evidence, groupTwoEvidence}, expected) {
		t.Fatal("exact group evidence was reported incomplete")
	}
	duplicate := evidence
	if rf3DiagnosticAuthorityEvidenceCovers([]raftmember.ReadAuthorityEvidence{evidence, duplicate}, expected) {
		t.Fatal("duplicate group evidence was reported complete")
	}
	wrongGroup := group
	wrongGroup.GroupID[0] = 10
	wrong := evidence
	wrong.Identity.Group = wrongGroup
	if rf3DiagnosticAuthorityEvidenceCovers([]raftmember.ReadAuthorityEvidence{evidence, wrong}, expected) {
		t.Fatal("wrong group evidence was reported complete")
	}
}
