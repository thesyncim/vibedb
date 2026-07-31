package storeio

import "fmt"

// Deterministic content-defined cuts for spanned exact-term leaves. One
// physical index's sorted term sequence is partitioned
// it into bounded leaves through three rules that are pure functions of the
// content itself — never of mutation history — so bulk build, incremental
// checkpoint folds, and journal replay of the same final postings produce the
// byte-identical leaf set (the invariant-4 identity anchor), and a content
// change reshapes only its own neighbourhood:
//
//  1. Term-boundary cuts: term T starts a new leaf iff the low sixteen bits
//     of its StoreID-keyed route hash fall under IndexTermLeafCutThreshold.
//     Cut terms delimit *runs*; everything inside one run is a pure function
//     of that run's own content, so a mutation can never reshape a leaf in a
//     different run.
//  2. Within-term stripe cuts: a *giant* term — one whose postings could not
//     fit even an empty leaf under the byte budget — is emitted as standalone
//     single-term piece leaves cut at fixed absolute tile boundaries (every
//     IndexTermLeafStripeTiles tiles). Stripe boundaries are absolute, so one
//     stripe's pieces depend only on that stripe's own postings; a mutation
//     into one stripe of a million-posting term re-encodes that stripe alone.
//     Empty stripes emit nothing.
//  3. Hard-cap cuts: a run of non-giant terms that would exceed the byte
//     budget is cut greedily at the last term that fits, and a single stripe
//     whose postings alone exceed the budget is cut at the last posting that
//     fits. A forced cut's position depends only on the run (or stripe) it is
//     inside and propagates no further than the next rule-1 or rule-2
//     boundary — still history-free, still local.
//
// Because the cutter can always cut, "term leaf exceeds MaxPageSize" is
// unreachable by construction for the primary graph's chunk-0 postings and is
// demoted to a corruption-class assertion at the page-staging layer.
//
// Sizing decisions use a deterministic *upper bound* estimate of the encoded
// leaf, not the exact builder output: the exact size depends on direct-block
// and dictionary selection, which would force a full trial encode per
// candidate cut. An upper bound preserves every safety property (an emitted
// leaf always fits) and is itself a pure function of content, so determinism
// and identity survive. The estimate error only ever cuts a leaf earlier than
// strictly necessary, which costs a few header bytes of space. The 10k-shape
// space test bounds that overhead to ≤ 5% over the single-leaf baseline.

const (
	// IndexTermLeafCutThreshold is the rule-1 probability numerator over a
	// 2^16 window: a term starts a new run iff routeHash & 0xFFFF falls under
	// it. 1365/65536 ≈ 1/48 targets a mean 48 terms per run — ≈ 4 KiB of
	// encoded leaf at the measured 10k-shape term cost; a run larger than the
	// byte budget is subdivided by rule 3.
	IndexTermLeafCutThreshold = 1365

	// IndexTermLeafStripeTiles is the fixed absolute tile width of one giant
	// term stripe. 2048 tiles of chunk-0 postings estimate to ≤ ~32 KiB, under
	// any admissible byte budget with the default 64 KiB MaxPageSize, so a
	// stripe is normally exactly one piece leaf; smaller budgets subdivide a
	// stripe through the rule-3 hard cap.
	IndexTermLeafStripeTiles = 2048

	// indexTermLeafAlignPad is the worst-case 8-byte alignment padding the
	// builder inserts ahead of a same-chunk global direct column.
	indexTermLeafAlignPad = 8
)

// IndexTermLeafCutBudget is the byte budget one emitted leaf must fit:
// the page envelope's payload ceiling clipped to the codec's own bound.
func IndexTermLeafCutBudget(maxPageSize uint32) int {
	budget := int(maxPageSize) - PageHeaderSize - PageTrailerSize
	if budget > IndexTermLeafMaxBytes {
		budget = IndexTermLeafMaxBytes
	}
	return budget
}

// IndexTermLeafRunCut is rule 1: does this term's route hash start a new run?
// The hash is the StoreID-keyed SipHash the term key record already carries,
// so run boundaries differ per store but are stable per (store, term).
func IndexTermLeafRunCut(routeHash uint64) bool {
	return routeHash&0xFFFF < IndexTermLeafCutThreshold
}

// IndexTermLeafStripe is the rule-2 stripe ordinal owning a posting tile.
func IndexTermLeafStripe(tileID uint32) uint32 {
	return tileID / IndexTermLeafStripeTiles
}

// IndexTermLeafEstimatePostingBytes upper-bounds one posting's encoded cost:
// kind byte, worst-case five-byte tile delta, and the largest representation
// the builder can select for it. A chunk-0 posting (the ordered primary
// graph's whole universe) lands on a direct encoding of at most one
// chunk+mask pair; a foreign posting is bounded by the two-mask direct form
// or the inline adaptive header plus its full payload.
func IndexTermLeafEstimatePostingBytes(p *IndexTermLeafPosting) int {
	if p.Chunk0Only {
		return 1 + 5 + 10 // kind + tile delta + DirectN(count=1, chunk+mask)
	}
	payload := len(p.Component)
	if payload < TermPostingInlineBytes {
		payload = TermPostingInlineBytes
	}
	direct := 1 + 2*9 // DirectN count byte + two chunk+mask pairs
	inline := 5 + payload
	worst := direct
	if inline > worst {
		worst = inline
	}
	return 1 + 5 + worst
}

// IndexTermLeafEstimateTermBytes upper-bounds one term's cost inside a leaf:
// its descriptor, its full key (prefix compression only shrinks), and its
// postings.
func IndexTermLeafEstimateTermBytes(t *IndexTermLeafTerm) int {
	n := indexTermLeafDescriptorBytes + len(t.Key.Canonical)
	for i := range t.Postings {
		n += IndexTermLeafEstimatePostingBytes(&t.Postings[i])
	}
	return n
}

// indexTermLeafEstimateLeafBytes upper-bounds a whole leaf holding termCount
// terms whose summed IndexTermLeafEstimateTermBytes is termBytes.
func indexTermLeafEstimateLeafBytes(termCount, termBytes int) int {
	return indexTermLeafHeaderBytes + indexTermLeafAlignPad +
		2*indexTermLeafEqualitySlots(termCount) + termBytes
}

// IndexTermLeafEstimateChunk0TermBytes is the term estimate for the ordered
// primary graph's shape — keyLen canonical bytes and postings chunk-0-only
// tile postings — computable without materializing the postings. It equals
// IndexTermLeafEstimateTermBytes for that shape exactly, which is what lets
// the fold's dirty classification apply the same giant predicate the cutter
// applies without resolving a term it may not re-encode.
func IndexTermLeafEstimateChunk0TermBytes(keyLen, postings int) int {
	return indexTermLeafDescriptorBytes + keyLen + postings*(1+5+10)
}

// IndexTermLeafGiant is rule 2's predicate: the term alone, in an otherwise
// empty leaf, would exceed the budget, so it must be emitted as stripe
// pieces. A pure function of the term's own content.
func IndexTermLeafGiant(termEstimate, budget int) bool {
	return indexTermLeafEstimateLeafBytes(1, termEstimate) > budget
}

// CutIndexTermLeaves partitions a strictly ordered term sequence into leaves
// obeying budget, emitting each leaf's term slice in order. Emitted packed
// slices alias terms; emitted piece slices alias a cutter-owned one-element
// buffer valid only for the duration of the emit call (with postings
// aliasing the giant term's posting slice). piece marks a giant-term stripe
// piece leaf. The decomposition is exactly composable: cutting any
// sub-sequence delimited by rule-1 cut terms and/or giant terms yields the
// same leaves the full cut yields for that range, which is what lets the
// checkpoint fold re-cut only dirty segments while preserving whole-index
// byte identity.
func CutIndexTermLeaves(
	terms []IndexTermLeafTerm,
	budget int,
	emit func(leaf []IndexTermLeafTerm, piece bool) error,
) error {
	var pieceBuf [1]IndexTermLeafTerm
	start := -1
	termCount, termBytes := 0, 0
	flush := func(end int) error {
		if start < 0 {
			return nil
		}
		leaf := terms[start:end]
		start, termCount, termBytes = -1, 0, 0
		return emit(leaf, false)
	}
	for i := range terms {
		term := &terms[i]
		if IndexTermLeafRunCut(term.Key.RouteHash) {
			if err := flush(i); err != nil {
				return err
			}
		}
		estimate := IndexTermLeafEstimateTermBytes(term)
		if IndexTermLeafGiant(estimate, budget) {
			if err := flush(i); err != nil {
				return err
			}
			if err := cutGiantIndexTerm(
				term, budget, &pieceBuf, emit,
			); err != nil {
				return err
			}
			continue
		}
		if start < 0 {
			start, termCount, termBytes = i, 1, estimate
			continue
		}
		if indexTermLeafEstimateLeafBytes(
			termCount+1, termBytes+estimate,
		) > budget {
			if err := flush(i); err != nil {
				return err
			}
			start, termCount, termBytes = i, 1, estimate
			continue
		}
		termCount++
		termBytes += estimate
	}
	return flush(len(terms))
}

// CutGiantIndexTerm emits one giant term as standalone piece leaves. The
// checkpoint fold calls it directly when patching single touched stripes of
// a term that is giant on both sides of the window: stripes are processed
// independently inside, so cutting one stripe's postings in isolation
// yields exactly the pieces a full cut yields for that stripe.
func CutGiantIndexTerm(
	term *IndexTermLeafTerm,
	budget int,
	emit func(leaf []IndexTermLeafTerm, piece bool) error,
) error {
	var pieceBuf [1]IndexTermLeafTerm
	return cutGiantIndexTerm(term, budget, &pieceBuf, emit)
}

// cutGiantIndexTerm emits one giant term as standalone piece leaves: postings
// grouped by absolute stripe, each stripe sub-cut at the last posting that
// fits (rule 3). Stripes are processed independently, so one stripe's pieces
// are a pure function of that stripe's own postings.
func cutGiantIndexTerm(
	term *IndexTermLeafTerm,
	budget int,
	pieceBuf *[1]IndexTermLeafTerm,
	emit func(leaf []IndexTermLeafTerm, piece bool) error,
) error {
	postings := term.Postings
	fixed := indexTermLeafDescriptorBytes + len(term.Key.Canonical)
	if len(postings) == 0 ||
		indexTermLeafEstimateLeafBytes(
			1, fixed+IndexTermLeafEstimatePostingBytes(&postings[0]),
		) > budget {
		return fmt.Errorf(
			"%w: exact term key and one posting exceed leaf budget",
			ErrInvalidWrite,
		)
	}
	for at := 0; at < len(postings); {
		stripe := IndexTermLeafStripe(postings[at].Posting.TileID)
		end := at + 1
		for end < len(postings) &&
			IndexTermLeafStripe(postings[end].Posting.TileID) == stripe {
			end++
		}
		for lo := at; lo < end; {
			hi := lo + 1
			pieceBytes := fixed +
				IndexTermLeafEstimatePostingBytes(&postings[lo])
			for hi < end {
				next := IndexTermLeafEstimatePostingBytes(&postings[hi])
				if indexTermLeafEstimateLeafBytes(
					1, pieceBytes+next,
				) > budget {
					break
				}
				pieceBytes += next
				hi++
			}
			pieceBuf[0] = IndexTermLeafTerm{
				Key: term.Key, Postings: postings[lo:hi:hi],
			}
			if err := emit(pieceBuf[:1], true); err != nil {
				return err
			}
			lo = hi
		}
		at = end
	}
	return nil
}
