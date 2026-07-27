; Narrow shape-tape span round-trip, proved over the full machine-word domain.
;
; Models docset_shape.go: a narrow shape-tape value packs a member's source
; span into one 32-bit word under the documented precondition that both offsets
; fit 16 bits (start, end <= shapeNarrowMaxEnd == #x0000ffff):
;
;   span    = start | (end << 16)
;   start() = span & #x0000ffff
;   end()   = span >> 16
;
; widen() rebuilds the wide entry from these halves (with next fixed at 1 and
; the info word carried verbatim), so the packing must lose nothing and the two
; 16-bit halves must never overlap.
;
; Claim: for all start, end in [0, 65535], span round-trips both halves and the
; halves are disjoint. We assert the negation and expect `unsat`.
(set-logic QF_BV)

(declare-const start (_ BitVec 32))
(declare-const end   (_ BitVec 32))

; The documented precondition: both offsets fit the 16-bit narrow width.
(assert (bvule start #x0000ffff))
(assert (bvule end   #x0000ffff))

(define-fun span () (_ BitVec 32) (bvor start (bvshl end #x00000010)))

; Negation: a half is not recovered, or the two halves share a set bit.
(assert (or (not (= (bvand  span #x0000ffff) start))
            (not (= (bvlshr span #x00000010) end))
            (not (= (bvand start (bvshl end #x00000010)) #x00000000))))

(check-sat)
