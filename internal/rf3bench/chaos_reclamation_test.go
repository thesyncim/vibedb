package rf3bench

import "testing"

func TestQualificationPreservesReclaimedWALMeasurements(t *testing.T) {
	q := validQualification()
	q.WALFinalBytes = q.WALBaselineBytes / 2
	q.WALGrowthBytes = 0
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	q.WALGrowthBytes = 1
	if err := q.Validate(); err == nil {
		t.Fatal("invented growth accepted after reclamation")
	}
	q.WALGrowthBytes, q.WALFinalBytes = 0, 0
	if err := q.Validate(); err == nil {
		t.Fatal("absent final WAL accepted")
	}
	q = validQualification()
	q.WALFinalBytes = q.WALBaselineBytes + WALGrowthBoundBytes + 1
	q.WALGrowthBytes = WALGrowthBoundBytes + 1
	if err := q.Validate(); err == nil {
		t.Fatal("growth above unchanged bound accepted")
	}
}
