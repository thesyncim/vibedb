package query

import (
	"regexp"
	"strings"
	"sync"
)

// pgRegexpCache caches compiled Go RE2 regular expressions keyed by
// (pattern, flags) so prepared predicates, joins, and scalar REGEXP_*
// functions never recompile or allocate per row on warm cache hits.
// When the cache reaches maxCachedRegexps, it clears and starts fresh to bound memory.
const maxCachedRegexps = 4096

type pgRegexpKey struct {
	pat   string
	flags string
}

var (
	pgRegexpCacheMu sync.RWMutex
	pgRegexpCache   = make(map[pgRegexpKey]*regexp.Regexp)
)

func cachedPGRegexp(pat, flags string) (*regexp.Regexp, error) {
	key := pgRegexpKey{pat: pat, flags: flags}
	pgRegexpCacheMu.RLock()
	re := pgRegexpCache[key]
	pgRegexpCacheMu.RUnlock()
	if re != nil {
		return re, nil
	}
	prefix := ""
	if strings.ContainsRune(flags, 'i') {
		prefix += "(?i)"
	}
	if strings.ContainsRune(flags, 'm') || strings.ContainsRune(flags, 'n') {
		prefix += "(?m)"
	}
	if strings.ContainsRune(flags, 's') {
		prefix += "(?s)"
	}
	re, err := regexp.Compile(prefix + pat)
	if err != nil {
		return nil, err
	}
	ownedKey := pgRegexpKey{pat: strings.Clone(pat), flags: strings.Clone(flags)}
	pgRegexpCacheMu.Lock()
	if existing := pgRegexpCache[ownedKey]; existing != nil {
		pgRegexpCacheMu.Unlock()
		return existing, nil
	}
	if len(pgRegexpCache) >= maxCachedRegexps {
		clear(pgRegexpCache)
	}
	pgRegexpCache[ownedKey] = re
	pgRegexpCacheMu.Unlock()
	return re, nil
}
