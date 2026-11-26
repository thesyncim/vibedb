package query

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// pgRegexpCache caches compiled Go RE2 regular expressions keyed by
// flags + "\x00" + pattern so prepared predicates, joins, and scalar
// REGEXP_* functions never recompile the same pattern per row.
// When the cache reaches maxEntries, it clears and starts fresh to bound memory.
const maxCachedRegexps = 4096

var (
	pgRegexpCache      sync.Map // string -> *regexp.Regexp
	pgRegexpCacheCount atomic.Int64
)

func cachedPGRegexp(pat, flags string) (*regexp.Regexp, error) {
	key := flags + "\x00" + pat
	if v, ok := pgRegexpCache.Load(key); ok {
		return v.(*regexp.Regexp), nil
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
	if pgRegexpCacheCount.Load() >= maxCachedRegexps {
		pgRegexpCache.Range(func(k, _ any) bool {
			pgRegexpCache.Delete(k)
			return true
		})
		pgRegexpCacheCount.Store(0)
	}
	if _, loaded := pgRegexpCache.LoadOrStore(key, re); !loaded {
		pgRegexpCacheCount.Add(1)
	}
	return re, nil
}
