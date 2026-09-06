package pgwire

import "strings"

// simpleCacheMaxEntries bounds the simple-query preparation cache. Entries
// retain one cloned SQL key plus a lowered plan each, so the table stays
// small while covering an application's repeated fixed-text statements
// (health checks, polls, dashboard refreshes).
const simpleCacheMaxEntries = 16

// cachedSimple returns the reusable simple preparation for text, or nil.
// Only runtime statements with no wire parameters are ever stored, so a hit
// skips protocol classification, parameter rewriting, semantic compilation,
// and RowDescription construction exactly like the unnamed-Parse fast path.
// The text may alias the reused message buffer; it is only compared here.
func (s *session) cachedSimple(text string) *prepared {
	stmt, ok := s.simpleCache[text]
	if !ok || stmt == nil || stmt.runtime == nil || stmt.wireParams != 0 {
		return nil
	}
	if s.cancelCheck != nil && s.cancelCheck() != nil {
		return nil
	}
	reuser, ok := stmt.runtime.(BackendStatementParseReuser)
	if !ok || !reuser.ReusableForParse() {
		return nil
	}
	return stmt
}

// prepareSimple prepares one simple statement, borrowing the session's
// cached preparation on an exact reusable hit. The release must run when the
// statement finishes: it is a no-op for a cache hit and releases a freshly
// prepared statement otherwise. A borrowed hit must never be released.
func (s *session) prepareSimple(text string) (stmt *prepared, release func(), err error) {
	if hit := s.cachedSimple(text); hit != nil {
		return hit, func() {}, nil
	}
	ownedText, ownsText := simpleCacheInput(text)
	prepared, err := s.prepare("", ownedText, nil)
	if err != nil {
		return nil, nil, err
	}
	if prepared.runtime == nil || prepared.wireParams != 0 || !ownsText {
		return prepared, prepared.release, nil
	}
	reuser, ok := prepared.runtime.(BackendStatementParseReuser)
	if !ok || !reuser.ReusableForParse() {
		return prepared, prepared.release, nil
	}
	if s.storeSimple(ownedText, prepared) {
		return prepared, func() {}, nil
	}
	return prepared, prepared.release, nil
}

// simpleCacheInput owns a bounded source before handing it to any backend
// compiler. Whether the preparation is a runtime statement, has wire
// parameters, and is reusable is known only after compilation; those checks
// decide whether this owned source enters the cache. Inputs too large even for
// the prepared-input budget stay on the ordinary borrowed path.
func simpleCacheInput(text string) (string, bool) {
	if preparedInputCharge("", text, 0) > maxPreparedInputBytes {
		return text, false
	}
	return strings.Clone(text), true
}

// storeSimple records a freshly prepared simple statement, evicting the
// oldest entries to fit the shared prepared-input bound. It reports whether
// the statement was stored; over-budget statements simply run uncached, so
// caching can never fail a query that would otherwise succeed.
func (s *session) storeSimple(text string, stmt *prepared) bool {
	if old, ok := s.simpleCache[text]; ok {
		// A generation boundary retired this entry; drop it before the
		// fresh preparation replaces the slot, keeping the byte budget
		// and the FIFO order exact.
		old.release()
		s.statementBytes -= old.retainedBytes
		delete(s.simpleCache, text)
		for i, key := range s.simpleOrder {
			if key == text {
				s.simpleOrder = append(s.simpleOrder[:i], s.simpleOrder[i+1:]...)
				break
			}
		}
	}
	charge := preparedInputCharge("", text, 0) + preparedDerivedCharge(stmt)
	if charge > maxPreparedInputBytes {
		return false
	}
	if s.simpleCache == nil {
		s.simpleCache = make(map[string]*prepared, simpleCacheMaxEntries)
	}
	for (len(s.simpleOrder) >= simpleCacheMaxEntries ||
		s.statementBytes+charge > maxPreparedInputBytes) && len(s.simpleOrder) > 0 {
		victim := s.simpleOrder[0]
		if old, ok := s.simpleCache[victim]; ok {
			old.release()
			s.statementBytes -= old.retainedBytes
		}
		delete(s.simpleCache, victim)
		s.simpleOrder = append(s.simpleOrder[:0], s.simpleOrder[1:]...)
	}
	// NOTE: the byte charge is returned to statementBytes only here at
	// eviction time. clearSimpleCache never adjusts the budget itself: both
	// of its callers (DISCARD ALL, session teardown) zero the budget around
	// it.
	if len(s.simpleOrder) >= simpleCacheMaxEntries ||
		s.statementBytes+charge > maxPreparedInputBytes {
		return false
	}
	// prepareSimple owns text before compiling and passes this same allocation
	// here, so the map key and every backend compiler that retained SQL share one
	// stable source rather than borrowing the reader's message buffer.
	key := text
	stmt.sql = text
	stmt.retainedBytes = charge
	s.simpleCache[key] = stmt
	s.simpleOrder = append(s.simpleOrder, key)
	s.statementBytes += charge
	return true
}

// clearSimpleCache releases every cached simple preparation. DISCARD ALL
// honors it like the named tables: a pooled client must be sure it got a
// clean connection.
func (s *session) clearSimpleCache() {
	for _, stmt := range s.simpleCache {
		stmt.release()
	}
	s.simpleCache = nil
	s.simpleOrder = nil
}
