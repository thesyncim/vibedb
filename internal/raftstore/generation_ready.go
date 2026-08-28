package raftstore

// The seal carries the durable source Ready cursor, not merely its process
// incarnation. Empty observed Readies need no WAL entry; the live owner retains
// their higher in-memory cursor. An idle later compaction carries the inherited
// sealed cursor until a new durable Ready or a new incarnation supersedes it.
func generationReadyFloor(current currentState, generation generationRecovery) uint64 {
	if current.retryPresent && current.retry.incarnation == current.currentIncarnation {
		return current.retry.readyID
	}
	if generation.present && generation.seal.sourceCurrentIncarnation == current.currentIncarnation {
		return generation.seal.sourceReadyID
	}
	return 0
}

func generationReadyAfterSource(seal generationSeal, incarnation, readyID uint64) bool {
	return readyID != 0 && (incarnation > seal.sourceCurrentIncarnation ||
		incarnation == seal.sourceCurrentIncarnation && readyID > seal.sourceReadyID)
}
