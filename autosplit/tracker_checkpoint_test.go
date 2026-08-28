package autosplit

import "testing"

func TestTrackerCheckpointRestoresExactLogicalCooldown(t *testing.T) {
	policy := TrackerPolicy{WindowCount: 2, RequiredWindows: 2, CooldownWindows: 3,
		MaxBoundaryDrift: 1, TriggerPressurePPM: 900_000}
	recommendation := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	recommendation.CurrentPressurePPM = 950_000
	var original Tracker
	if original.Observe(recommendation, policy) {
		t.Fatal("first evidence window qualified")
	}
	recommendation.WindowSequence = 2
	if !original.Observe(recommendation, policy) {
		t.Fatal("second evidence window did not qualify")
	}
	restored, ok := RestoreTracker(original.Checkpoint())
	if !ok {
		t.Fatal("valid tracker checkpoint rejected")
	}
	for sequence := uint64(3); sequence <= 7; sequence++ {
		recommendation.WindowSequence = sequence
		want := original.Observe(recommendation, policy)
		if got := restored.Observe(recommendation, policy); got != want ||
			restored.Checkpoint() != original.Checkpoint() {
			t.Fatalf("restored tracker diverged at logical window %d: got=%t want=%t", sequence, got, want)
		}
	}
}

func TestRestoreTrackerRejectsManufacturedHistory(t *testing.T) {
	recommendation := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	recommendation.CurrentPressurePPM = 950_000
	var tracker Tracker
	tracker.Observe(recommendation, DefaultTrackerPolicy())
	checkpoint := tracker.Checkpoint()
	checkpoint.Seen = 1
	checkpoint.History = 2
	if _, ok := RestoreTracker(checkpoint); ok {
		t.Fatal("restore accepted history outside the observed logical window count")
	}
	checkpoint = tracker.Checkpoint()
	checkpoint.Stable = false
	if _, ok := RestoreTracker(checkpoint); ok {
		t.Fatal("restore accepted an anchor while marked unstable")
	}
}
