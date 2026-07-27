package query

import (
	"strings"

	"github.com/thesyncim/vibedb/store"
)

// Chunk-summary (zone map) candidate generation.
//
// This is the planner half of store/store_zone.go. Its whole job is to turn a
// compiled leaf predicate into a [store.ZoneProbe] once, at compile time, so
// that execution costs one walk of the chunk headers and nothing per document.
// Everything about soundness lives on the store side; what matters here is
// that the zone tier is offered *only* where a wider-than-true candidate set
// is acceptable:
//
//   - never when requireExact is set. A zone mask is a superset by
//     construction, and NOT complements its child against the live universe,
//     where a superset child would remove real matches.
//   - never as a substitute for a declared index. An index answers the same
//     leaf exactly and at row granularity; the zone tier fills in only where
//     the catalog has nothing, which before this file meant a full scan.
//
// Because a zone mask is a sound superset, adding it can only shrink the row
// set the executor rechecks — never change which rows the recheck accepts.
// That is what makes the pruned and unpruned plans return identical results,
// and it is asserted directly by the differential tests rather than argued.

// zoneOpFor maps a comparison operator onto its chunk-summary form. Every
// operator has one: even Ne, whose bounds cannot exclude anything, still
// prunes a chunk in which the path is always null or always absent, because a
// null cell fails every comparison including inequality.
func zoneOpFor(op Op) store.ZoneOp {
	switch op {
	case Eq:
		return store.ZoneOpEq
	case Ne:
		return store.ZoneOpNe
	case Lt:
		return store.ZoneOpLt
	case Le:
		return store.ZoneOpLe
	case Gt:
		return store.ZoneOpGt
	case Ge:
		return store.ZoneOpGe
	default:
		return store.ZoneOpNone
	}
}

// zoneCodeOf returns the chunk-summary code for a classified literal. A null
// literal has none: compareScalar orders null below every other kind, but
// evalCmp rejects a null *cell* outright, so a comparison against a null
// literal has no bound the summary can express and is left unpruned.
func zoneCodeOf(s scalar) (uint32, bool) {
	switch s.kind {
	case kindBool:
		return store.ZoneCodeBool(s.bval), true
	case kindNumber:
		return store.ZoneCodeNumber(s.num)
	case kindString:
		return store.ZoneCodeString(s.sval), true
	default:
		return 0, false
	}
}

// zoneName returns the decoded top-level member name a leaf reads, and false
// for any path the summary does not track. A dotted single-segment path
// carries its decoded name already; a JSON Pointer spelling of the same path
// is accepted when it has exactly one unescaped segment, so `/price` and
// `price` plan identically. Anything nested, or any segment carrying a ~0/~1
// escape, is declined rather than guessed at.
func zoneName(p *compiledPredicate, paths []compiledPath) (string, bool) {
	if p.boundPath != "" {
		return zoneNameFromPointer(p.boundPath)
	}
	if p.col < 0 || p.col >= len(paths) {
		return "", false
	}
	if cp := paths[p.col]; cp.single {
		return cp.name, true
	}
	return zoneNameFromPointer(paths[p.col].indexPath())
}

func zoneNameFromPointer(pointer string) (string, bool) {
	if len(pointer) < 2 || pointer[0] != '/' {
		return "", false
	}
	segment := pointer[1:]
	if strings.ContainsAny(segment, "/~") {
		return "", false
	}
	return segment, true
}

// attachZoneProbe compiles a leaf's chunk-summary probe, or leaves the zero
// (inert) probe in place. It runs once per compilation, never per execution.
func attachZoneProbe(cp *compiledPredicate, paths []compiledPath) {
	name, ok := zoneName(cp, paths)
	if !ok {
		return
	}
	hash := store.ZonePathHash(name)
	switch cp.kind {
	case predCmp:
		op := zoneOpFor(cp.op)
		if op == store.ZoneOpNone {
			return
		}
		lit := cp.lit
		if cp.col < 0 {
			// A containment-derived equality carries its literal as a needle
			// index instead of a registered column, because the leaf exists
			// only to choose an index and must never be evaluated per row.
			// Classifying the needle root recovers the same scalar the row
			// evaluator would have compared against.
			lit = classifyRaw(cp.needle.Root().Raw())
		}
		code, ok := zoneCodeOf(lit)
		if !ok {
			return
		}
		cp.zone = store.ZoneProbe{Path: hash, Lo: code, Hi: code, Op: op}
	case predIn:
		if len(cp.lits) == 0 {
			return
		}
		// The alternatives are already sorted by compareScalar and the code is
		// monotone over that order, so the first and last would do. The loop
		// runs anyway because it also proves every alternative *has* a code:
		// one that does not would leave the interval silently too narrow, and
		// a too-narrow equality interval is precisely a false negative.
		lo, hi := ^uint32(0), uint32(0)
		for _, lit := range cp.lits {
			code, ok := zoneCodeOf(lit)
			if !ok {
				return
			}
			lo = min(lo, code)
			hi = max(hi, code)
		}
		cp.zone = store.ZoneProbe{Path: hash, Lo: lo, Hi: hi, Op: store.ZoneOpEq}
	case predExists:
		cp.zone = store.ZoneProbe{Path: hash, Op: store.ZoneOpExists}
	case predIsNull:
		cp.zone = store.ZoneProbe{Path: hash, Op: store.ZoneOpIsNull}
	}
}

// zoneCandidates probes the chunk summaries for one leaf, returning the same
// (masks, bounded, exact, error) shape as every other candidate producer. It
// reports exact false unconditionally: a chunk mask admits every live row of a
// surviving chunk, so the compiled predicate must still recheck each one.
func zoneCandidates(p *compiledPredicate, zone store.ZoneSource, w *Workspace, requireExact bool) ([]store.Mask, bool, bool, error) {
	if requireExact || zone == nil || p.zone.Op == store.ZoneOpNone {
		return nil, false, false, nil
	}
	out := w.nextStoreMasks()
	out, pruned, bounded := zone.AppendZoneMasks(out, p.zone)
	w.keepStoreMasks(out)
	if !bounded {
		return nil, false, false, nil
	}
	w.zonePruned += pruned
	return out, true, false, nil
}
