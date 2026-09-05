Frozen Sol transport review handoff

Worktree: /private/tmp/vibedb-fused-rf3-node
No stage/commit or timed benchmarks performed. All files below are frozen.

Concrete fixes beyond Luna's draft:
- Socket SQL replies now encode the semantic SQL result only at EncodeReplicatedResponse; local typed SQL requests and replies avoid nested frames.
- Exact semantic SQL frame admission includes both length/max-value fields (the draft omitted four bytes).
- Local SubmitOwned command clones are private AND capacity-clamped. The real Linux owner rejected slices.Clone's rounded capacity until this was fixed.
- Legacy local native SQL bytes are admitted before their inner decoder runs. Nested SQL request/result tag and exact declared lengths are checked against the enclosing payload before allocation; malformed inflated inner headers reject allocation-free.
- Local and remote semantic result validation share canonical field, cardinality, enum, fence, and size checks. Endpoint member/store/incarnation mismatch releases the local reply lease before returning an error.
- Detached results clone byte fields, column names, error strings, and position strings, preserving nil-row shape. Read leases and frame/SQL credits remain held through detachment or network encoding.
- Storage destination and gateway principal must differ, mutually validate certificate chains/roots/build/trust domain, and satisfy the actual native allowlist. Cached opaque profile proofs validate full-chain expiry on every local call without hot-path certificate parsing.
- Storage rotation cancels active local work and invalidates old bindings. Rebinding the same identities works while serving and cancels calls using the previous binding. After Rotate, call BindLocalGatewayPeerTLS again to resume local service. The listener must use the identical ReplicatedServerTLS capability that was bound locally.
- Closed servers reject local dispatch; listener shutdown cancels local executions. Caller/server/SQL deadlines and shared SQL quotas/native headroom are tested.
- LinearizableDataReadRequest now carries the existing owner read authorization callback, so live/transitional serving authority is rechecked at serialized ReadIndex admission after Probe. Quorum, applied, generation and durability barriers are unchanged.
- ReplicatedNodeClient.Stats exposes bounded atomic LocalCalls, RemoteCalls, SemanticSQLCalls, LegacyCalls, SQLRequestEncodings and SQLRequestEncodedBytes. Encoded byte counts are actual emitted inner request bytes, never estimates. LegacyCalls counts the DoReplicated API including ordinary native writes/point reads; SemanticSQLCalls counts typed SQL. Custom semantic remote transports are not credited with inferred encodings.

Files for parent integration (including frozen inherited Luna edits):
/private/tmp/vibedb-fused-rf3-node/gateway/replicated_query.go
/private/tmp/vibedb-fused-rf3-node/gateway/replicated_query_test.go
/private/tmp/vibedb-fused-rf3-node/gateway/replicated_semantic.go
/private/tmp/vibedb-fused-rf3-node/gateway/replicated_semantic_test.go
/private/tmp/vibedb-fused-rf3-node/shardservice/codec.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_query.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_server.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_tls.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_tls_profile_test.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_wire.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_dispatch.go
/private/tmp/vibedb-fused-rf3-node/shardservice/replicated_dispatch_test.go
/private/tmp/vibedb-fused-rf3-node/internal/rafttransport/identity.go
/private/tmp/vibedb-fused-rf3-node/internal/rafttransport/identity_test.go
/private/tmp/vibedb-fused-rf3-node/internal/raftservice/data_read.go
/private/tmp/vibedb-fused-rf3-node/internal/raftservice/owner_rf3_semantic_test.go

Validation:
1. Full go test ./gateway ./shardservice ./internal/rafttransport ./internal/raftservice -count=1 passed on Go 1.27. Final log: /private/tmp/fused-transport-sol-complete-final.log
2. Focused macOS race gate passed. Log: /private/tmp/fused-transport-sol-final-race.log
3. Final Linux arm64 Go 1.27 race gate passed across all four packages, selecting real RF3 local/TLS owner tests plus semantic ownership/admission/auth/expiry/stats/bounded-frame tests. Strict allocation was required using VIBEDB_RF3_QUORUM_QUALIFICATION=1. Source/module cache were mounted read-only; retained store files used Docker volume vibedb-fused-transport-sol-20260904-review with fresh t.TempDir paths. Final log: /private/tmp/fused-transport-sol-linux-bounded-final.log
4. Verbose Linux evidence for output bounds and isolated local-owner quorum refusal: /private/tmp/fused-transport-sol-linux-final.log
5. git diff --check passed for all owned directories.

Real Linux test TestRF3SemanticLocalTLSQueriesAndRevocation uses the existing production three-voter fixture, real SQL apply source and owner ReadIndex. It commits rows via gateway NativeSession; compares local typed SQL against authenticated TLS for point hit/miss/empty field, scan, grouped aggregate and SQL error; tests stale schema, output limits, Probe-to-ReadIndex revocation, authenticated transition, retained results after write/reuse, isolated local-owner refusal and zero leaked credits. TestRF3SQLReadUsesAcceptedFenceWithoutPostProbe also passed.

Remaining scope/limits:
- No performance or full fused-process promotion claim. The broader three/six-node multi-group kill/restart/retirement campaign remains a parent task gate. This added real transport fixture uses one RF3 group and three voters; it does not replace the fused multi-process campaign or explicit schema retirement testing.
- Production direct-write/coordinated-fallback protocols are unchanged and their existing gateway suites passed. The added real differential fixture commits ordinary NativeSession mutations, not every direct-issuer/fallback failure permutation.
- Final consumer compile encountered other-agent runtime WIP: runtime_table_catalogs.go:42 needs a pointer for vibejson.Marshal(paths); runtime_control.go:10 has an unused gateway import. cmd/vibedb compiled. These were relayed to root and runtime reviewer without cross-edits. Log: /private/tmp/fused-transport-sol-consumer-final.log
