package pgwire

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	// DefaultMaxStatementStats bounds the number of distinct normalized SQL
	// fingerprints retained by pg_stat_statements per Server.
	DefaultMaxStatementStats = 1024

	// DisableStatementStats opts out of pg_stat_statements collection when set
	// on Options.MaxStatementStats.
	DisableStatementStats = -1

	// maxStatementStatQueryBytes caps the retained normalized SQL text per
	// fingerprint so oversized ad-hoc statements cannot inflate memory.
	maxStatementStatQueryBytes = 4096

	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

type stmtStatsKey struct {
	database string
	user     string
	queryID  int64
}

type stmtStatsEntry struct {
	key   stmtStatsKey
	query string

	calls      atomic.Uint64
	totalNanos atomic.Uint64
	minNanos   atomic.Uint64
	maxNanos   atomic.Uint64
	rows       atomic.Int64
	_          [64]byte // prevent false sharing across hot entries
}

func (e *stmtStatsEntry) record(dur time.Duration, rows int64) {
	var nanos uint64
	if dur > 0 {
		nanos = uint64(dur.Nanoseconds())
	}
	e.calls.Add(1)
	e.totalNanos.Add(nanos)
	if rows > 0 {
		e.rows.Add(rows)
	}
	for {
		old := e.minNanos.Load()
		if nanos >= old {
			break
		}
		if e.minNanos.CompareAndSwap(old, nanos) {
			break
		}
	}
	for {
		old := e.maxNanos.Load()
		if nanos <= old {
			break
		}
		if e.maxNanos.CompareAndSwap(old, nanos) {
			break
		}
	}
}

type stmtStatsTable struct {
	entries map[stmtStatsKey]*stmtStatsEntry
}

// stmtStatsRegistry maintains bounded, per-fingerprint SQL execution
// statistics for pg_stat_statements. Steady-state recording of known
// fingerprints is completely lock-free and allocation-free (1 atomic pointer
// load + map lookup + atomic counter updates). Cold-path insertions,
// evictions, and resets publish a new copy-on-write snapshot under mu.
type stmtStatsRegistry struct {
	table      atomic.Pointer[stmtStatsTable]
	mu         sync.Mutex
	maxEntries int
}

type stmtStatsSnapshot struct {
	queryID       int64
	datname       string
	usename       string
	calls         uint64
	totalNanos    uint64
	totalExecTime float64 // milliseconds
	minExecTime   float64 // milliseconds
	maxExecTime   float64 // milliseconds
	meanExecTime  float64 // milliseconds
	rows          int64
	query         string
}

func newStmtStatsRegistry(maxEntries int) *stmtStatsRegistry {
	if maxEntries <= 0 {
		return nil
	}
	r := &stmtStatsRegistry{maxEntries: maxEntries}
	r.table.Store(&stmtStatsTable{})
	return r
}

func (r *stmtStatsRegistry) record(database, user, rawSQL string, dur time.Duration, rows int64) {
	if r == nil || r.maxEntries <= 0 || rawSQL == "" {
		return
	}
	var stack [256]byte
	queryID, norm := normalizeSQLForStats(stack[:0], rawSQL)
	if len(norm) == 0 {
		return
	}
	key := stmtStatsKey{database: database, user: user, queryID: queryID}

	// Lock-free, zero-allocation fast path for known statement fingerprints.
	if tbl := r.table.Load(); tbl != nil {
		if entry, ok := tbl.entries[key]; ok {
			entry.record(dur, rows)
			return
		}
	}

	// Cold path: insert new fingerprint via copy-on-write map publication.
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.table.Load()
	var curEntries map[stmtStatsKey]*stmtStatsEntry
	if cur != nil {
		curEntries = cur.entries
		if entry, ok := curEntries[key]; ok {
			entry.record(dur, rows)
			return
		}
	}

	var victimKey stmtStatsKey
	evict := len(curEntries) >= r.maxEntries
	if evict {
		var victimCalls uint64 = math.MaxUint64
		var victimTotal uint64 = math.MaxUint64
		first := true
		for k, e := range curEntries {
			c := e.calls.Load()
			tot := e.totalNanos.Load()
			if first || c < victimCalls || (c == victimCalls && (tot < victimTotal || (tot == victimTotal && k.queryID < victimKey.queryID))) {
				first = false
				victimKey = k
				victimCalls = c
				victimTotal = tot
			}
		}
	}

	nextCap := len(curEntries) + 1
	if evict {
		nextCap = len(curEntries)
	}
	nextEntries := make(map[stmtStatsKey]*stmtStatsEntry, nextCap)
	for k, e := range curEntries {
		if evict && k == victimKey {
			continue
		}
		nextEntries[k] = e
	}

	entry := &stmtStatsEntry{
		key:   key,
		query: truncateUTF8Bytes(norm, maxStatementStatQueryBytes),
	}
	entry.minNanos.Store(math.MaxUint64)
	entry.record(dur, rows)
	nextEntries[key] = entry
	r.table.Store(&stmtStatsTable{entries: nextEntries})
}

func (r *stmtStatsRegistry) reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.table.Store(&stmtStatsTable{})
	r.mu.Unlock()
}

func (r *stmtStatsRegistry) snapshot() []stmtStatsSnapshot {
	if r == nil {
		return nil
	}
	tbl := r.table.Load()
	if tbl == nil || len(tbl.entries) == 0 {
		return nil
	}
	out := make([]stmtStatsSnapshot, 0, len(tbl.entries))
	for _, e := range tbl.entries {
		calls := e.calls.Load()
		if calls == 0 {
			continue
		}
		totalNanos := e.totalNanos.Load()
		minNanos := e.minNanos.Load()
		if minNanos == math.MaxUint64 {
			minNanos = 0
		}
		maxNanos := e.maxNanos.Load()
		totalMs := float64(totalNanos) / 1e6
		out = append(out, stmtStatsSnapshot{
			queryID:       e.key.queryID,
			datname:       e.key.database,
			usename:       e.key.user,
			calls:         calls,
			totalNanos:    totalNanos,
			totalExecTime: totalMs,
			minExecTime:   float64(minNanos) / 1e6,
			maxExecTime:   float64(maxNanos) / 1e6,
			meanExecTime:  totalMs / float64(calls),
			rows:          e.rows.Load(),
			query:         e.query,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].totalNanos != out[j].totalNanos {
			return out[i].totalNanos > out[j].totalNanos
		}
		if out[i].calls != out[j].calls {
			return out[i].calls > out[j].calls
		}
		return out[i].queryID < out[j].queryID
	})
	return out
}

// normalizeSQLForStats normalizes SQL text for pg_stat_statements fingerprinting:
//   - strips line (-- ...) and nested block (/* ... */) comments
//   - collapses runs of whitespace to a single ASCII space
//   - replaces single-quoted strings, E'...' strings, dollar-quoted strings,
//     and standalone numeric literals with "$?"
//   - preserves double-quoted identifiers and positional parameters ($1, $2, ?)
//   - trims leading/trailing whitespace and trailing semicolons
//
// It appends into dst (typically a stack buffer) to avoid heap allocations on
// the steady-state hot path, and returns a non-zero 64-bit FNV-1a queryID.
func normalizeSQLForStats(dst []byte, raw string) (int64, []byte) {
	n := len(raw)
	i := 0
	pendingSpace := false
	prevWasIdent := false

	emitSpace := func() {
		if len(dst) > 0 {
			pendingSpace = true
		}
		prevWasIdent = false
	}
	flushSpace := func() {
		if pendingSpace && len(dst) > 0 {
			dst = append(dst, ' ')
			pendingSpace = false
		}
	}

	for i < n {
		c := raw[i]

		// Whitespace
		if isSQLSpace(c) {
			emitSpace()
			i++
			continue
		}

		// Line comment: -- ...\n
		if c == '-' && i+1 < n && raw[i+1] == '-' {
			emitSpace()
			i += 2
			for i < n && raw[i] != '\n' {
				i++
			}
			continue
		}

		// Block comment (supports PostgreSQL nesting): /* ... */
		if c == '/' && i+1 < n && raw[i+1] == '*' {
			emitSpace()
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if raw[i] == '/' && i+1 < n && raw[i+1] == '*' {
					depth++
					i += 2
				} else if raw[i] == '*' && i+1 < n && raw[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
			continue
		}

		// Escaped string literal: E'...' or e'...'
		if (c == 'E' || c == 'e') && !prevWasIdent && i+1 < n && raw[i+1] == '\'' {
			flushSpace()
			dst = append(dst, '$', '?')
			i += 2
			for i < n {
				if raw[i] == '\\' && i+1 < n {
					i += 2
					continue
				}
				if raw[i] == '\'' {
					i++
					if i < n && raw[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			prevWasIdent = false
			continue
		}

		// Single-quoted string literal: '...'
		if c == '\'' {
			flushSpace()
			dst = append(dst, '$', '?')
			i++
			for i < n {
				if raw[i] == '\'' {
					i++
					if i < n && raw[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			prevWasIdent = false
			continue
		}

		// Double-quoted identifier: "..."
		if c == '"' {
			flushSpace()
			start := i
			i++
			for i < n {
				if raw[i] == '"' {
					i++
					if i < n && raw[i] == '"' {
						i++
						continue
					}
					break
				}
				i++
			}
			dst = append(dst, raw[start:i]...)
			prevWasIdent = true
			continue
		}

		// Dollar-quoted string ($tag$...$tag$) or positional parameter ($1)
		if c == '$' {
			if endTag, ok := matchDollarQuoteTag(raw, i); ok {
				tag := raw[i:endTag]
				flushSpace()
				dst = append(dst, '$', '?')
				i = endTag
				for i < n {
					if raw[i] == '$' && i+len(tag) <= n && raw[i:i+len(tag)] == tag {
						i += len(tag)
						break
					}
					i++
				}
				prevWasIdent = false
				continue
			}
			flushSpace()
			dst = append(dst, '$')
			i++
			for i < n && raw[i] >= '0' && raw[i] <= '9' {
				dst = append(dst, raw[i])
				i++
			}
			prevWasIdent = true
			continue
		}

		// Standalone numeric literal (not part of an identifier such as col1).
		if !prevWasIdent && ((c >= '0' && c <= '9') || (c == '.' && i+1 < n && raw[i+1] >= '0' && raw[i+1] <= '9')) {
			flushSpace()
			dst = append(dst, '$', '?')
			i = skipSQLNumber(raw, i)
			prevWasIdent = false
			continue
		}

		// Identifier or keyword character
		if isIdentChar(c) {
			flushSpace()
			dst = append(dst, c)
			i++
			prevWasIdent = true
			continue
		}

		// Punctuation / operator
		flushSpace()
		dst = append(dst, c)
		i++
		prevWasIdent = false
	}

	// Trim trailing whitespace and semicolons.
	for len(dst) > 0 && (dst[len(dst)-1] == ';' || isSQLSpace(dst[len(dst)-1])) {
		dst = dst[:len(dst)-1]
	}
	if len(dst) == 0 {
		return 0, dst
	}

	h := uint64(fnvOffset64)
	for _, b := range dst {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	id := int64(h)
	if id == 0 {
		id = 1
	}
	return id, dst
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c >= 0x80
}

func matchDollarQuoteTag(s string, pos int) (int, bool) {
	n := len(s)
	if pos >= n || s[pos] != '$' {
		return 0, false
	}
	if pos+1 < n && s[pos+1] == '$' {
		return pos + 2, true
	}
	j := pos + 1
	if j >= n || !((s[j] >= 'a' && s[j] <= 'z') || (s[j] >= 'A' && s[j] <= 'Z') || s[j] == '_') {
		return 0, false
	}
	j++
	for j < n && isIdentChar(s[j]) {
		j++
	}
	if j < n && s[j] == '$' {
		return j + 1, true
	}
	return 0, false
}

func skipSQLNumber(s string, i int) int {
	n := len(s)
	// Hexadecimal literal 0x...
	if i+1 < n && s[i] == '0' && (s[i+1] == 'x' || s[i+1] == 'X') {
		i += 2
		for i < n && ((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f') || (s[i] >= 'A' && s[i] <= 'F') || s[i] == '_') {
			i++
		}
		return i
	}
	for i < n && ((s[i] >= '0' && s[i] <= '9') || s[i] == '_') {
		i++
	}
	if i < n && s[i] == '.' {
		i++
		for i < n && ((s[i] >= '0' && s[i] <= '9') || s[i] == '_') {
			i++
		}
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < n && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < n && s[j] >= '0' && s[j] <= '9' {
			i = j + 1
			for i < n && ((s[i] >= '0' && s[i] <= '9') || s[i] == '_') {
				i++
			}
		}
	}
	return i
}

func truncateUTF8Bytes(b []byte, maxBytes int) string {
	if len(b) <= maxBytes {
		return string(b)
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(b[end]) {
		end--
	}
	return string(b[:end])
}
