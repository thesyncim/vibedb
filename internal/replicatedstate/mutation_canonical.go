package replicatedstate

import (
	"bytes"
	"errors"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Canonical values borrow command bytes. Rewritten values share one arena for
// the entire logical plan, not one arena per relation: previous relation slices
// remain live until capture, logical hashing and durable staging have finished.
type canonicalMutationScratch struct {
	entries []vibejson.IndexEntry
	work    storeio.CanonicalWorkspace
	arena   []byte
	command replication.CommandView
	rows    []TransactionRelationPayloadView
	budget  int
	ready   bool
}

const maxCanonicalMutationBytes = 2 * replication.MaxCommandBytes

func (s *canonicalMutationScratch) begin(command replication.CommandView, rows []TransactionRelationPayloadView) {
	s.command, s.rows = command, rows
	s.arena = s.arena[:0]
	s.budget, s.ready = 0, false
}

func (s *canonicalMutationScratch) release() {
	s.command = replication.CommandView{}
	s.rows = nil
	s.budget, s.ready = 0, false
	s.arena = s.arena[:0]
	if uint64(cap(s.arena)+cap(s.entries)*int(unsafe.Sizeof(vibejson.IndexEntry{})))+s.work.CapacityBytes() > maxNormalBatchRetainedBufferBytes {
		s.arena, s.entries = nil, nil
		s.work = storeio.CanonicalWorkspace{}
	}
}

// canonicalMutationUpperBytes is the renderer's exact conservative expansion
// bound. Number spellings are deliberately preserved. Only raw U+2028/U+2029
// can expand (three UTF-8 bytes become six escape bytes), including object keys.
func canonicalMutationUpperBytes(value []byte) int {
	return len(value) + 3*(bytes.Count(value, []byte("\xe2\x80\xa8"))+bytes.Count(value, []byte("\xe2\x80\xa9")))
}

// Reserve lazily on the first rewrite, using JSON value bytes rather than the
// whole command or global-index payloads. The validated command/stored
// transaction is bounded by MaxCommandBytes; check that invariant here too.
// No published slice can be invalidated by a later arena growth.
func (m *Machine) reserveCanonicalMutations() bool {
	s := &m.canonicalMutations
	if s.ready {
		return true
	}
	budget := 0
	add := func(batch replication.RelationBatchView) bool {
		ordinal := int(batch.Relation) - 1
		if ordinal < 0 || ordinal >= len(m.relations) {
			// The normal planner reports UnknownRelation at that batch. A
			// size-only lookahead must not change deterministic result order.
			return true
		}
		if m.relations[ordinal].kind != RelationJSON {
			return true
		}
		iterator := batch.Mutations()
		for iterator.Next() {
			mutation := iterator.Mutation()
			if mutation.Kind == replication.MutationDelete || mutation.Kind == replication.MutationDeleteDigestEqual {
				continue
			}
			bound := canonicalMutationUpperBytes(mutation.Value)
			if bound > maxCanonicalMutationBytes-budget {
				return false
			}
			budget += bound
		}
		return true
	}
	if s.rows != nil {
		for _, row := range s.rows {
			if !add(row.Batch) {
				return false
			}
		}
	} else {
		iterator := s.command.RelationBatches()
		for iterator.Next() {
			if !add(iterator.Batch()) {
				return false
			}
		}
	}
	if cap(s.arena) < budget {
		s.arena = make([]byte, 0, budget)
	}
	s.budget, s.ready = budget, true
	return true
}

func (m *Machine) canonicalMutationValue(value []byte) ([]byte, uint32) {
	s := &m.canonicalMutations
	if len(value) > replication.MaxCommandBytes {
		return nil, ResultTargetBound
	}
	if cap(s.entries) == 0 {
		s.entries = make([]vibejson.IndexEntry, 0, 64)
	}
	var index vibejson.Index
	for {
		var err error
		index, err = vibejson.BuildIndex(value, s.entries[:cap(s.entries)])
		if !errors.Is(err, document.ErrIndexFull) {
			if err != nil {
				return nil, ResultInvalidDocument
			}
			break
		}
		// Each tape entry consumes at least one input byte. Bound growth even
		// for malformed, extremely tape-dense input.
		if cap(s.entries) >= len(value)+1 {
			return nil, ResultInvalidDocument
		}
		s.entries = make([]vibejson.IndexEntry, 0, min(2*cap(s.entries), len(value)+1))
	}
	s.entries = index.Entries
	if storeio.IndexIsCanonical(index, &s.work) {
		return value, ResultApplied
	}
	if !m.reserveCanonicalMutations() || canonicalMutationUpperBytes(value) > s.budget-len(s.arena) {
		return nil, ResultTargetBound
	}
	start := len(s.arena)
	out, err := storeio.AppendCanonicalIndexed(s.arena[:start:s.budget], index, &s.work)
	if err != nil {
		return nil, ResultInvalidDocument
	}
	if len(out) > s.budget || cap(out) != s.budget {
		// Defensive contract check: never publish slices from an unexpected
		// renderer allocation while earlier relation slices remain live.
		return nil, ResultTargetBound
	}
	s.arena = out
	return out[start:len(out):len(out)], ResultApplied
}
