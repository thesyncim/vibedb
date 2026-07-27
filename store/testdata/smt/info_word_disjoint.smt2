; Info-word field partition, proved as a machine-checked statement.
;
; The three info-word fields must be pairwise disjoint and cover all 32 bits;
; that partition is what lets setCount and bumpCount rewrite the count field
; without disturbing kind or flags, and what lets packInfo compose the fields
; with a bare bitwise or (index.go).
;
;   count mask #x03ffffff   kind mask #x1c000000   flags mask #xe0000000
;
; Claim: the masks are pairwise disjoint and their union is the whole word. We
; assert the negation of that conjunction and expect `unsat`.
(set-logic QF_BV)

(assert (not (and (= (bvand #x03ffffff #x1c000000) #x00000000)
                  (= (bvand #x03ffffff #xe0000000) #x00000000)
                  (= (bvand #x1c000000 #xe0000000) #x00000000)
                  (= (bvor  #x03ffffff (bvor #x1c000000 #xe0000000)) #xffffffff))))

(check-sat)
