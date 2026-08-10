package txnclock

import "strings"

// ExternalHistory is bounded first-committer-wins history whose revisions are
// assigned by an external database-global sequencer. One history belongs to one
// collection, so its string keys need no collection-name encoding.
//
// LastWrite supports coarse collection dependencies. Floor marks exact point
// dependencies begun before a bounded-history reset as conflicting. Writes owns
// every retained key and therefore never pins a caller's transaction arena.
type ExternalHistory struct {
	LastWrite uint64
	Floor     uint64
	Writes    map[string]uint64
}

// ConflictPoint reports whether key changed after begin, or whether this
// collection's exact history overflowed after begin.
func (h *ExternalHistory) ConflictPoint(begin uint64, key string) bool {
	if h == nil {
		return false
	}
	if h.Floor != 0 && begin < h.Floor {
		return true
	}
	return h.Writes[key] > begin
}

// ConflictCollection reports whether any key in the collection changed after
// begin. It does not depend on exact key history and therefore cannot overflow.
func (h *ExternalHistory) ConflictCollection(begin uint64) bool {
	return h != nil && h.LastWrite > begin
}

// RecordAt records keys at an externally assigned revision. oldest is the
// oldest currently active begin revision. Obsolete exact entries are rebuilt
// only when needed to avoid an overflow, keeping the common overwrite path
// constant-time while preserving the hard HistoryKeys bound.
func (h *ExternalHistory) RecordAt(revision, oldest uint64, keys []string) {
	h.recordAt(revision, oldest, keys, false)
}

// RecordUniqueAt is RecordAt for callers that guarantee keys has no duplicate
// elements. Transaction publication order already has that property, allowing
// history admission to remain linear in the published key count.
func (h *ExternalHistory) RecordUniqueAt(revision, oldest uint64, keys []string) {
	h.recordAt(revision, oldest, keys, true)
}

func (h *ExternalHistory) recordAt(
	revision, oldest uint64,
	keys []string,
	unique bool,
) {
	if h == nil || len(keys) == 0 {
		return
	}
	h.LastWrite = revision
	if h.Floor != 0 && oldest >= h.Floor {
		h.Floor = 0
	}

	newKeys := countNewExternalKeys(h.Writes, keys, unique)
	if len(h.Writes)+newKeys > HistoryKeys {
		h.prune(oldest)
		newKeys = countNewExternalKeys(h.Writes, keys, unique)
		if len(h.Writes)+newKeys > HistoryKeys {
			h.Floor = revision
			h.Writes = nil
			return
		}
	}
	if h.Writes == nil {
		h.Writes = make(map[string]uint64, newKeys)
	}
	for _, key := range keys {
		if _, exists := h.Writes[key]; exists {
			h.Writes[key] = revision
			continue
		}
		h.Writes[strings.Clone(key)] = revision
	}
}

func (h *ExternalHistory) prune(oldest uint64) {
	remaining := 0
	for _, revision := range h.Writes {
		if revision > oldest {
			remaining++
		}
	}
	if remaining == len(h.Writes) {
		return
	}
	if remaining == 0 {
		h.Writes = nil
		return
	}
	writes := make(map[string]uint64, remaining)
	for key, revision := range h.Writes {
		if revision > oldest {
			writes[key] = revision
		}
	}
	h.Writes = writes
}

func countNewExternalKeys(
	writes map[string]uint64,
	keys []string,
	unique bool,
) int {
	count := 0
	for i, key := range keys {
		if _, exists := writes[key]; exists {
			continue
		}
		if !unique {
			duplicate := false
			for j := 0; j < i; j++ {
				if keys[j] == key {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		count++
	}
	return count
}
