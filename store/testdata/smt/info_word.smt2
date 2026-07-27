; Info-word packing round-trip, proved over the full machine-word domain.
;
; Models index.go: packInfo composes an entry's count, kind, and flags into one
; 32-bit info word, and Count/Kind/flags recover them:
;
;   count : bits [0,25]   mask #x03ffffff  (infoCountMask == infoMaxCount)
;   kind  : bits [26,28]  shift #x0000001a (infoKindShift = 26), mask #x1c000000
;   flags : bits [29,31]  shift #x0000001d (infoFlagsShift = 29)
;
;   packInfo(count,kind,flags) =
;       (count & #x03ffffff) | (kind << 26) | (flags << 29)
;   Count(w) = w & #x03ffffff
;   Kind(w)  = (w & #x1c000000) >> 26
;   flags(w) = w >> 29
;
; Claim: for every count <= infoMaxCount and every 3-bit kind and flags, the
; accessors recover each field exactly. We assert the negation and expect z3 to
; report `unsat`: no in-range triple breaks the round-trip, so packing loses no
; information and no field bleeds into another.
(set-logic QF_BV)

(declare-const count (_ BitVec 32))
(declare-const kind  (_ BitVec 32))
(declare-const flags (_ BitVec 32))

; The ranges packInfo's callers guarantee (index.go: the builders reject counts
; past infoMaxCount; only three flag bits and seven kinds are defined).
(assert (bvule count #x03ffffff))
(assert (bvult kind  #x00000008))
(assert (bvult flags #x00000008))

(define-fun pack () (_ BitVec 32)
  (bvor (bvand count #x03ffffff)
        (bvor (bvshl kind  #x0000001a)
              (bvshl flags #x0000001d))))

(define-fun getCount () (_ BitVec 32) (bvand pack #x03ffffff))
(define-fun getKind  () (_ BitVec 32) (bvlshr (bvand pack #x1c000000) #x0000001a))
(define-fun getFlags () (_ BitVec 32) (bvlshr pack #x0000001d))

; Negation of the round-trip: some field is not recovered.
(assert (or (not (= getCount count))
            (not (= getKind  kind))
            (not (= getFlags flags))))

(check-sat)
