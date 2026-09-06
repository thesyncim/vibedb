package raftstore

// partitionHintCandidates is bounded by the already collected wave. Collection
// admits at most one submission per group and stops at every control request,
// so moving these independent groups cannot cross a same-group or control
// ordering barrier. Durable submissions keep their mutual order and batch.
func partitionHintCandidates(items *[MaxPersistGroupBatches]*Submission, count int) int {
	if count < 2 || items[0].kind != submissionReady {
		return 0
	}
	var ordered [MaxPersistGroupBatches]*Submission
	hints := 0
	for i := 0; i < count; i++ {
		if submissionHintCandidate(items[i]) {
			ordered[hints] = items[i]
			hints++
		}
	}
	if hints == 0 || hints == count {
		return hints
	}
	next := hints
	for i := 0; i < count; i++ {
		if !submissionHintCandidate(items[i]) {
			ordered[next] = items[i]
			next++
		}
	}
	copy(items[:count], ordered[:count])
	return hints
}

func submissionHintCandidate(s *Submission) bool {
	if s == nil || s.kind != submissionReady {
		return false
	}
	ready := s.nodeReady()
	for i := 0; i < nodeReadySeriesCount(ready); i++ {
		batch := nodeReadySeriesBatch(&ready, i)
		if batch.MustSync || len(batch.Entries) != 0 || !canonicalEmptySnapshot(batch.Snapshot) {
			return false
		}
	}
	return true
}
