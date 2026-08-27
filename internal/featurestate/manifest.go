// Package featurestate owns the evidence-backed distributed feature ledger.
//
// The manifest is the source of truth for docs/distributed-feature-state.md.
// It distinguishes code that exists from code that is integrated, exposed by
// the repository's commands, and qualified under a fault or benchmark gate.
package featurestate

//go:generate go run ./cmd/featurestategen -out ../../docs/distributed-feature-state.md

type Status uint8

const (
	StatusNo Status = iota
	StatusPartial
	StatusYes
)

func (s Status) label() string {
	switch s {
	case StatusYes:
		return "Yes"
	case StatusPartial:
		return "Partial"
	default:
		return "No"
	}
}

type Reference struct {
	Path   string
	Symbol string
}

type Stage struct {
	Status   Status
	Contract string
	Evidence []Reference
}

type Feature struct {
	Name          string
	Primitive     Stage
	Integrated    Stage
	Shipped       Stage
	Qualification Stage
}

func ref(path, symbol string) Reference {
	return Reference{Path: path, Symbol: symbol}
}

// Distributed is deliberately conservative. A correctness test is not a
// fault or benchmark gate. A package used only by another internal package is
// not a shipped command. No row predicts work from an open branch.
var Distributed = []Feature{
	{
		Name: "Static shard routing and scatter reads",
		Primitive: Stage{StatusYes, "The catalog, planner, router, merge path, and bounded fanout exist.", []Reference{
			ref("gateway/catalog.go", "Snapshot"), ref("gateway/executor.go", "Executor"),
		}},
		Integrated: Stage{StatusYes, "The gateway executor sends SQL to leader-only shard services and merges complete results.", []Reference{
			ref("gateway/e2e_test.go", "TestE2EFanoutShapes"), ref("shardservice/server.go", "Server"),
		}},
		Shipped: Stage{StatusYes, "vibedb-gateway serve and vibedb-shard serve expose this static-ownership path.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("cmd/vibedb-shard/main.go", "runServe"),
		}},
		Qualification: Stage{StatusPartial, "Correctness, stale-catalog, admission, and merge tests exist. There is no external kill or scaling benchmark gate for this command path.", []Reference{
			ref("gateway/e2e_test.go", "TestE2EFanoutShapes"), ref("gateway/executor_test.go", "TestExecutorFailClosedOnShardError"),
		}},
	},
	{
		Name: "Authenticated and authorized service transport",
		Primitive: Stage{StatusYes, "TLS 1.3 profiles bind node identity, traffic class, trust roots, and limits. Canonical vibejson policies bind exact principals and generations to fixed capabilities.", []Reference{
			ref("internal/servicetls/server.go", "Server"), ref("internal/serviceauthz/policy.go", "Policy"),
		}},
		Integrated: Stage{StatusYes, "The gateway authorizes complete request semantics and forwards the exact client authority. Shards independently check it while requiring a delegate-capable gateway.", []Reference{
			ref("gateway/client_tls.go", "ServeAuthorizedClients"), ref("shardservice/server.go", "ServeAuthorizedConn"),
		}},
		Shipped: Stage{StatusYes, "The gateway and static shard commands require TLS plus an authorization policy unless the operator selects explicit loopback development plaintext. The RF3 shard command is always authenticated.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("cmd/vibedb-shard/main.go", "runServe"), ref("cmd/vibedb-shard/serve_rf3.go", "runServeRF3"),
		}},
		Qualification: Stage{StatusPartial, "Identity, generation rotation, confused-deputy, SQL classification, connection, and allocation tests exist. Real TCP TLS gates cover client-to-gateway handshake churn and gateway-to-shard independent delegate and forwarded-principal checks. An external shard process gate sustains requests across a directional partition and healing, rotates its certificate generation while revoking the old stream, and rejects both a rogue gateway certificate and a confused-deputy request. Hot authorization is allocation-free; the steady-stream benchmark keeps the standard crypto/tls per-record read allocation floor separate and visible. The complete gateway command process is not yet part of this external gate.", []Reference{
			ref("gateway/client_tls_test.go", "TestClientTLSAuthenticatesAuthorizesRotatesAndSeparatesALPN"), ref("gateway/client_tls_qualification_test.go", "TestAuthorizedClientTLSNetworkChaosAndThroughputGate"), ref("gateway/client_tls_qualification_test.go", "BenchmarkAuthorizedClientTLSRequest"), ref("gateway/shard_tls_qualification_test.go", "TestAuthenticatedShardBoundaryRotationAndConfusedDeputyFault"), ref("gateway/shard_tls_qualification_test.go", "TestAuthenticatedGatewayShardHotAuthorizationAllocationFree"), ref("gateway/shard_tls_qualification_test.go", "BenchmarkAuthenticatedGatewayShardStream"), ref("gateway/shard_tls_process_qualification_test.go", "TestAuthenticatedGatewayShardProcessPartitionRotationAndDeputyFaults"), ref("shardservice/authorization_test.go", "TestShardAuthorizationRejectsConfusedDeputyAndSeparatesRoles"),
		}},
	},
	{
		Name: "Coherent multi-shard reads",
		Primitive: Stage{StatusYes, "Bounded static read fences establish one scoped vector cut. RF3 exact-key batch reads use one leader ReadIndex cut per group and return an explicit sorted observation vector; neither contract claims one global MVCC timestamp.", []Reference{
			ref("gateway/read_snapshot.go", "snapshotFanout"), ref("gateway/replicated_sql_read.go", "ReadSQLBatch"),
		}},
		Integrated: Stage{StatusYes, "The static gateway acquires all fences before fanout. The RF3 reader lowers multiple tables against one pinned catalog, folds relations by group, bounds parallel group reads and response bytes, and refuses every partial result.", []Reference{
			ref("gateway/read_snapshot.go", "snapshotFanout"), ref("gateway/replicated_sql_read.go", "ReadSQLBatch"),
		}},
		Shipped: Stage{StatusYes, "The query operation serves the static vector-cut contract. The read_batch operation serves ordered multi-table and multi-group RF3 exact-primary-key reads and emits the per-group observation vector.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "execRequest"), ref("cmd/vibedb-gateway/data_sql_batch.go", "buildNativeSQLBatchReadRequest"),
		}},
		Qualification: Stage{StatusPartial, "Static lease, cleanup, stale-refusal, and pinned-snapshot tests exist. RF3 command tests prove same-group multi-relation cuts, cross-group vectors, all-or-nothing byte admission, and bounded execution across 65 groups. There is no external partition or long-running tail-latency gate.", []Reference{
			ref("shardservice/read_fence_test.go", "TestReadFenceLeaseExpiresAndWakesWriter"), ref("cmd/vibedb-gateway/data_sql_batch_test.go", "TestReadBatchWireSixtyFiveGroupsIsBoundedAndUncapped"), ref("cmd/vibedb-gateway/data_sql_batch_test.go", "TestReadBatchWireExactByteBoundAndIntentNeverReturnPartialValues"),
		}},
	},
	{
		Name: "Distributed clock model and skew resilience",
		Primitive: Stage{StatusYes, "RF3 order and reads use Raft term, index, applied index, and ReadOnlySafe. Transaction recovery advances bounded replicated pulses. Execution-pin leases and hot-shard cooldown use replicated progress fences instead of elapsed time.", []Reference{
			ref("internal/raftmodel/config.go", "NewConfig"), ref("internal/executionpin/command.go", "Command"), ref("internal/distributedtxn/journal.go", "Journal"),
		}},
		Integrated: Stage{StatusYes, "Static and RF3 transaction recovery require an ordered pulse sequence before abort. The durable request lifecycle binds one clockless controller epoch and applied-index lease to the complete program. Pressure evidence uses catalog generations and authority revisions.", []Reference{
			ref("gateway/recovery.go", "recoverCoordinator"), ref("gateway/replicated_request_execution_context.go", "BuildDurableRequestExecutionPinBinding"), ref("internal/hotshard/controller.go", "Controller"),
		}},
		Shipped: Stage{StatusYes, "The RF3 command path uses quorum order, applied-index reads, vector cuts, logical recovery pulses, and the shipped durable request service's applied-index execution pins. Local time controls only TLS validity, network/context deadlines, retry scheduling, catalog-session deadline construction, and the separate static read-fence lane.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "execRequest"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("internal/rafttransport/identity.go", "PeerTLS"),
		}},
		Qualification: Stage{StatusPartial, "A bounded Linux command matrix composes independently injected peer UTC steps and TLS validity, logical-pulse restart recovery, two-group leader isolation and exact transaction retry, real process suspend/resume, former-leader refusal, foreground failover latency, and the shipped serve-rf3 kill/partition pressure gate. Skips fail the matrix and evidence bytes are bounded. Live database-process UTC is not changed, and arbitrary static read-fence suspension/overrun remains unqualified, so this is not a global timestamp or unrestricted clock-fault claim.", []Reference{
			ref("internal/rafttransport/clock_fault_matrix_test.go", "TestPeerTLSIndependentUTCStepMatrix"), ref("gateway/recovery_test.go", "TestRecoveryManifestMissingPageRequiresLogicalPulsesAcrossRestart"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestTwoRealRF3GroupsExecuteFusedTwoParticipantTransactionAcrossLeaderIsolation"), ref("internal/raftservice/process_rf3_test.go", "TestRF3NativeServingThreeProcessRecoveryEvidence"), ref("cmd/vibedb-shard/serve_rf3_fault_process_test.go", "TestServeRF3ShippedFaultHarness"),
		}},
	},
	{
		Name: "Byte-bounded distributed transactions",
		Primitive: Stage{StatusYes, "Compact inline and root-bound paged coordinator manifests implement prepare, decision, apply, release, retry, and bounded recovery without a participant-count contract.", []Reference{
			ref("internal/distributedtxn/manifest.go", "ManifestBuilder"), ref("gateway/transaction.go", "executeTransaction"),
		}},
		Integrated: Stage{StatusYes, "ExecBatch and global-index writes select the inline fast path or stream segmented manifests through the authenticated shard protocol and paged recovery.", []Reference{
			ref("gateway/transaction_manifest.go", "stageTransactionCoordinator"), ref("gateway/recovery.go", "recoverManifestCoordinator"),
		}},
		Shipped: Stage{StatusYes, "The static gateway serves multi-table and cross-shard mutation batches under byte, mutation, deadline, and in-flight bounds and runs recovery at startup and on a timer.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("gateway/recovery.go", "RecoverAll"),
		}},
		Qualification: Stage{StatusPartial, "A real 65-shard gateway transaction proves the segmented path, readback, and outcome-unknown handling. Multi-page restart, malformed-page, idempotency, recovery, and failure-atomicity tests also exist. External multi-process kill and partition gates do not cover this static command path.", []Reference{
			ref("gateway/segmented_e2e_test.go", "TestSegmentedExecBatchAcross65RealShardServers"), ref("gateway/segmented_e2e_test.go", "TestSegmentedCoordinatorResponseLossAndRestartBoundaries"),
		}},
	},
	{
		Name: "Fused multi-shard RF3 transaction orchestration",
		Primitive: Stage{StatusYes, "Fresh replicated operations atomically combine coordinator begin with its local prepare, remote stage with prepare, and participant apply or abort with release. Inline and greedily packed manifests remain byte-bounded without a participant-count contract.", []Reference{
			ref("internal/distributedtxn/replicated_codec.go", "ReplicatedOperation"), ref("internal/replicatedstate/transaction_apply.go", "planCoordinatorBeginPrepare"),
		}},
		Integrated: Stage{StatusYes, "The durable request runner streams the sealed participant program, builds exact fused commands, stages their bytes before proposal, and recovers from replicated ReadIndex witnesses. Aggregate execution-pin epochs fence takeover locally at every participant while one home-group proof admits each persisted wave.", []Reference{
			ref("gateway/replicated_request_transaction_runner.go", "DurableRequestDistributedRunner"), ref("gateway/replicated_request_lifecycle_runner.go", "RunWave"), ref("gateway/replicated_transaction_protocol.go", "replicatedTransactionCommandEncoder"),
		}},
		Shipped: Stage{StatusYes, "vibedb-gateway accepts only authenticated structured issuer identities for RF3 exec_batch, lowers one or more tables and global-index relations, performs fused durable admission, and returns the terminal result with an authenticated ACK capability. There is no process-local request registry or legacy RF3 fallback.", []Reference{
			ref("gateway/durable_sql_request_executor.go", "Execute"), ref("cmd/vibedb-gateway/durable_request_adapter.go", "ExecBatch"), ref("cmd/vibedb-gateway/serve.go", "newReplicatedCatalogGateway"),
		}},
		Qualification: Stage{StatusPartial, "State-machine, schedule, durable lifecycle, and exact-retry gates cover atomic transitions, bounded reclamation, concurrent gateways, and the fused 2P+1 decision/apply schedule without a participant-count contract. Real RF3 gates prove two independently led data groups, a dedicated request-ledger group, isolation, exact hidden-command retry, replacement-gateway terminal replay, ACK recovery, replica convergence, and former-leader refusal. The full composition is in-process with a typed catalog snapshot and net.Pipe native edge. No child-process gateway plus catalog-RF3 kill/partition gate exists yet.", []Reference{
			ref("internal/replication/transaction_perf_contract_test.go", "TestReplicatedTransactionEncodedSchedulePerformanceTargets"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestTwoRealRF3GroupsExecuteFusedTwoParticipantTransactionAcrossLeaderIsolation"), ref("internal/raftservice/owner_rf3_request_ledger_test.go", "TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions"), ref("gateway/replicated_request_ledger_fault_test.go", "TestDurableRequestReplacementRecoversEveryReplicatedBoundary"),
		}},
	},
	{
		Name: "RF3 transaction recovery reads",
		Primitive: Stage{StatusYes, "A closed hidden-state reader provides exact coordinator and participant lookup, paged manifest access, and bounded resumable active-coordinator scans without a participant-count contract.", []Reference{
			ref("internal/replicatedstate/transaction_recovery_read.go", "TransactionRecoveryReadInto"), ref("internal/replicatedstate/transaction_recovery_read.go", "TransactionRecoveryReadRequest"),
		}},
		Integrated: Stage{StatusYes, "The dedicated transaction-recovery capability, leader-only ReadIndex path, native shard protocol, and leader-aware gateway executor share exact byte, row, applied-index, and serving-fence bounds. The durable distributed runner consumes those reads while replaying its sealed participant stream.", []Reference{
			ref("gateway/replicated_transaction_recovery.go", "ReadTransactionRecovery"), ref("gateway/replicated_request_transaction_runner.go", "recoverManifestDescriptor"),
		}},
		Shipped: Stage{StatusYes, "vibedb-shard serve-rf3 installs the authenticated recovery reader. vibedb-gateway retries structured requests from replicated ledger state, reuses the sealed program after admission, and exposes terminal ACK plus bounded collection. Recovery authority is replicated; the command installs no same-process registry or periodic process-memory sweep.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("gateway/durable_sql_request_executor.go", "Replay"), ref("gateway/replicated_request_service.go", "Acknowledge"),
		}},
		Qualification: Stage{StatusPartial, "Real RF3 tests prove leader-only recovery, replacement-leader continuity, isolated-former-leader refusal, hidden-commit recovery across two groups, terminal replay through a replacement gateway, and lost committed ACK recovery through a new ledger leader. Durable fault tests cover bounded state, duplicate convergence, ACK tombstones, and restart at every lifecycle cut. Full external child-process gateway replacement remains unqualified.", []Reference{
			ref("internal/raftservice/owner_rf3_transaction_test.go", "TestRF3TransactionRecoveryReadIsLeaderOnlyAndSurvivesGatewayReplacement"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestTwoRealRF3GroupsExecuteFusedTwoParticipantTransactionAcrossLeaderIsolation"), ref("internal/raftservice/owner_rf3_request_ledger_test.go", "TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions"), ref("gateway/replicated_request_ledger_fault_test.go", "TestDurableRequestLostResponseConvergesWhenAnotherGatewayAdvancesAhead"),
		}},
	},
	{
		Name: "Durable RF3 request ledger",
		Primitive: Stage{StatusYes, "A replicated request grammar owns paged plans, pending waves, terminal results, acknowledgements, bounded collection, issuer lanes, contiguous issuer high-water, and logical execution pins. Catalog metadata stores adjacent immutable ledger-home ranges with exact RF3 route authority.", []Reference{
			ref("internal/requestledger/command.go", "Command"), ref("gateway/replicated_request_ledger.go", "DurableRequestLedgerTopology"), ref("internal/executionpin/command.go", "Command"),
		}},
		Integrated: Stage{StatusYes, "The typed service selects the catalog-persisted ledger home, seals and streams a logical transaction recipe, recovers lifecycle CAS operations from RF3 state, fences every transaction wave with one execution-pin epoch, derives stable ACK authority, and collects only contiguous GC-complete issuer sequences.", []Reference{
			ref("gateway/replicated_request_service.go", "NewDurableRequestService"), ref("gateway/replicated_request_lifecycle_runner.go", "NewDurableRequestLifecycleRunner"), ref("gateway/replicated_request_issuer_collector.go", "NewDurableIssuerHighwaterCollector"),
		}},
		Shipped: Stage{StatusYes, "vibedb cluster dev provisions a dedicated request-ledger group and shared ACK authority. runServe constructs the RF3 ledger client, catalog-bound topology, execution-pin sessions, distributed runner, replicated issuer authority, strict structured exec_batch service, and ACK collector. Missing durable authority fails startup; no legacy fallback is installed.", []Reference{
			ref("cmd/vibedb/cluster_dev.go", "ensureDevCluster"), ref("cmd/vibedb-gateway/durable_request_runtime.go", "newReplicatedDurableRuntime"), ref("cmd/vibedb-gateway/serve.go", "handleConnPolicyDurable"),
		}},
		Qualification: Stage{StatusPartial, "Internal tests prove catalog topology round trips, bounded wide-plan replay, logical-pin fencing, concurrent-gateway convergence, ACK and GC recovery, issuer high-water restart, and strict public wire identity. One real three-voter ledger gate proves lost-create recovery and contiguous sequencing. A production-composed in-process gate adds two user-data RF3 groups, full SQL execution, terminal response loss, replacement-gateway replay, authenticated ACK refusal, lost committed ACK recovery, and write-free completed retry. Its native edge uses net.Pipe and its catalog is a shared typed snapshot, so no external child-process gateway plus catalog-RF3 qualification claim is made.", []Reference{
			ref("gateway/replicated_request_ledger_catalog_test.go", "TestRequestLedgerTopologyCatalogRoundTripAndExactRouteBinding"), ref("gateway/replicated_request_ledger_fault_test.go", "TestDurableRequestConcurrentGatewaysConvergeOnOneOutcome"), ref("gateway/replicated_request_issuer_collector_test.go", "TestDurableIssuerHighwaterCollectorResolvesOutcomeUnknownAndRestart"), ref("internal/raftservice/owner_rf3_request_ledger_test.go", "TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition"), ref("internal/raftservice/owner_rf3_request_ledger_test.go", "TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions"),
		}},
	},
	{
		Name: "Global exact index routing and maintenance",
		Primitive: Stage{StatusYes, "Catalog metadata, independent index placement, exact lookup, lifecycle fencing, and write expansion exist.", []Reference{
			ref("gateway/index_metadata.go", "IndexMetadata"), ref("gateway/global_index.go", "GlobalIndexProgram"),
		}},
		Integrated: Stage{StatusYes, "Static planning can select a global index. Static and RF3 writes add independently placed index participants without forcing the base row onto the index shard. RF3 update and delete bind index removal to the exact prior base value.", []Reference{
			ref("gateway/global_index_test.go", "TestGlobalIndexRoutesIndexAndBaseIndependently"), ref("gateway/replicated_sql_transaction.go", "planReplicatedSQLTransaction"),
		}},
		Shipped: Stage{StatusYes, "The static gateway consumes ready global-index metadata and exposes exact lookup. The RF3 exec_batch lane lowers ready unique and non-unique global-index maintenance into relation-aware transaction participants.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("gateway/replicated_sql_transaction.go", "planReplicatedSQLTransaction"),
		}},
		Qualification: Stage{StatusPartial, "Tests cover routing, lifecycle, byte preservation, rollback, stale split fences, exact old-value checks, same-key replacement, and the independently led multi-group RF3 transaction substrate. There is no external index-churn, leader-failure, or performance gate.", []Reference{
			ref("gateway/replicated_sql_transaction_test.go", "TestReplicatedSQLTransactionRoutesReadyGlobalIndexAsIndependentRF3Participant"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestTwoRealRF3GroupsExecuteFusedTwoParticipantTransactionAcrossLeaderIsolation"),
		}},
	},
	{
		Name: "Atomic multi-relation replicated apply",
		Primitive: Stage{StatusYes, "One replicated command can bind and atomically apply dense JSON and global-index relation batches with one result.", []Reference{
			ref("internal/replicatedstate/relation_bundle.go", "RelationCollection"), ref("internal/replication/command.go", "RelationBatchView"),
		}},
		Integrated: Stage{StatusYes, "The replicated state machine validates the relation manifest and commits all relation targets with its system state.", []Reference{
			ref("internal/replicatedstate/machine.go", "Machine"), ref("internal/replicatedstate/relation_bundle_test.go", "TestRelationBundleAtomicBaseLocalAndGlobalIndexApply"),
		}},
		Shipped: Stage{StatusYes, "vibedb-shard prepare-rf3 creates and serve-rf3 opens an exact replicated SQL/apply bundle. vibedb-gateway can lower exact-key mutations for multiple RF3 base-table relations into the same participant without table or SQL strings entering Raft.", []Reference{
			ref("cmd/vibedb-shard/prepare_rf3.go", "runPrepareRF3"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("gateway/replicated_sql_transaction.go", "planReplicatedSQLTransaction"),
		}},
		Qualification: Stage{StatusPartial, "Deterministic apply, replay, malformed-command, failure-atomic, multi-table lowering, and RF3 global-index lowering tests exist. No external multi-relation fault or sustained-load gate exists.", []Reference{
			ref("internal/replicatedstate/relation_bundle_test.go", "TestRelationBundleCheckpointCrashPhasesNeverRecoverSkew"), ref("gateway/replicated_sql_transaction_test.go", "TestReplicatedSQLTransactionRoutesReadyGlobalIndexAsIndependentRF3Participant"),
		}},
	},
	{
		Name: "RF3 proposal serving and exact retry",
		Primitive: Stage{StatusYes, "Bounded proposal admission, Raft quorum, durable committed apply, settlement waiters, and request identity exist.", []Reference{
			ref("internal/raftservice/owner.go", "Owner"), ref("internal/raftserve/registry.go", "Waiter"),
		}},
		Integrated: Stage{StatusYes, "Authenticated peer runtime, replicated shard service, and native gateway executor form a complete internal RF3 path.", []Reference{
			ref("internal/raftservice/peer.go", "AuthenticatedPeerRuntime"), ref("gateway/replicated_native.go", "ReplicatedExecutor"),
		}},
		Shipped: Stage{StatusYes, "vibedb-shard prepare-rf3 atomically creates one stable three-voter member root. serve-rf3 opens either that singleton manifest or 1..64 retained group bundles, routes each group through one of 1..64 deterministic execution lanes, shares one authenticated per-peer transport across all lanes, and serves the authenticated native endpoint. Multi-group enrolled replacement targets remain rejected until snapshot listeners are group-scoped.", []Reference{
			ref("cmd/vibedb-shard/prepare_rf3.go", "runPrepareRF3"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"),
		}},
		Qualification: Stage{StatusPartial, "Preparation and manifest gates prove restartable artifact publication, overwrite refusal, canonical multi-group parsing, and group-scaled serving bounds. A shipped-composition three-process gate proves retained-state opening, mutual TLS, natural election, authenticated reads, and clean process shutdown. Internal gates additionally enumerate all eight three-voter reachability masks, prove majority commit and minority fail-closed behavior, benchmark execution-lane scaling and hot-group isolation, and cover follower catch-up, response loss, exact retry, and acknowledged-result survival. A multi-group shipped-process scaling gate and exhaustive external quorum/apply cuts remain absent.", []Reference{
			ref("cmd/vibedb-shard/prepare_rf3_test.go", "TestPrepareRF3PublishesCompleteRestartableMemberAndReopensExactly"), ref("cmd/vibedb-shard/rf3_manifest_test.go", "TestParseRF3ManifestCanonicalMultiGroupBundles"), ref("cmd/vibedb-shard/serve_rf3_test.go", "TestRF3MultiGroupServingLimitsCoverManifestBound"), ref("cmd/vibedb-shard/serve_rf3_process_test.go", "TestServeRF3ShippedCompositionThreeProcesses"), ref("internal/raftservice/process_rf3_test.go", "TestRF3NativeServingThreeProcessRecoveryEvidence"), ref("internal/raftservice/owner_rf3_test.go", "TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestRF3AllThreeVoterQuorumCutsFailClosedOrCommit"), ref("internal/multiraft/lanes_test.go", "BenchmarkExecutionLanesScaling"), ref("internal/multiraft/lanes_test.go", "BenchmarkExecutionLanesSixtyFourGroups"), ref("internal/multiraft/lanes_test.go", "BenchmarkExecutionLanesHotShardIsolation"),
		}},
	},
	{
		Name: "Development cluster and Kubernetes test tooling",
		Primitive: Stage{StatusYes, "A local RF1 development/no-HA or RF3 process orchestrator and deterministic Helm-free Kubernetes manifest renderer exist.", []Reference{
			ref("cmd/vibedb/cluster_dev.go", "runClusterDev"), ref("internal/kubeoperator/render.go", "Render"),
		}},
		Integrated: Stage{StatusYes, "The local command generates credentials, policy, WAL and shared ACK material, portable schema witnesses, and distinct retained catalog, request-ledger, and data roots. RF3 supervises three voters per role plus one gateway. RF1 supervises one no-HA member per role without a gateway. Catalog genesis stores one immutable proof atomically with the first head and witness. The Kubernetes lane composes stable DNS, PVCs, disruption budgets, shard and gateway StatefulSets, and a scale-zero learner bootstrap template.", []Reference{
			ref("cmd/vibedb/cluster_dev.go", "ensureDevCluster"), ref("internal/kubeoperator/render.go", "Render"),
		}},
		Shipped: Stage{StatusYes, "vibedb cluster dev --replicas 1|3 starts or resumes the same three-role topology. RF1 has three single-voter role processes and no gateway. RF3 has nine retained members across independent catalog, request-ledger, and data groups plus one gateway. vibedb-operator render and prepare provide deterministic Kubernetes test manifests and idempotent ordinal preparation. The renderer is not a reconciliation watch-loop, and Kubernetes DNS is discovery rather than leader or topology authority.", []Reference{
			ref("cmd/vibedb/main.go", "run"), ref("cmd/vibedb-operator/main.go", "render"), ref("cmd/vibedb-operator/main.go", "prepare"),
		}},
		Qualification: Stage{StatusPartial, "Tests prove canonical local-cluster resume, three independent apply roles, portable replica schema witnesses, ledger-only capacity, data-only table publication, explicit RF1/RF3 validation, production-policy compatibility, child reaping, distinct loopback endpoints, deterministic Kubernetes output, and injection rejection. There is no retained prior-binary fixture or negotiated mixed-format protocol, so no honest rolling-format compatibility gate can be constructed yet. End-to-end Kubernetes storage, DNS, and rolling-restart faults also remain absent.", []Reference{
			ref("cmd/vibedb/cluster_dev_test.go", "TestDevClusterManifestResumeIsCanonicalAndDoesNotReprovision"), ref("cmd/vibedb/cluster_dev_test.go", "TestInitializeDevClusterEmitsThreeIndependentApplyRoles"), ref("cmd/vibedb/cluster_dev_test.go", "TestDevRequestLedgerPrepareProfileMatchesCatalogHomeAndKeepsCatalogDisabled"), ref("cmd/vibedb/cluster_dev_test.go", "TestDevReplicatedTableProfileUsesPortableSchemaAcrossReplicaLocalStores"), ref("cmd/vibedb/cluster_dev_test.go", "TestDevCatalogPublishesOnlyThePortableDataTableProfile"), ref("internal/kubeoperator/render_test.go", "TestRenderRF3GoldenAndSafetyContract"), ref("internal/kubeoperator/render_test.go", "TestRenderRejectsInvalidIdentityAndInjection"),
		}},
	},
	{
		Name: "Automatic Raft WAL retention",
		Primitive: Stage{StatusYes, "Checkpoint-bound WAL generation preparation, authenticated selection, publication, settlement, and old-generation replacement exist.", []Reference{
			ref("internal/raftstore/generation_activate.go", "GenerationActivation"), ref("internal/raftmember/generation_driver.go", "WALGenerationDriverOptions"),
		}},
		Integrated: Stage{StatusYes, "Each RF3 runtime captures and prepares generation authority on its owner lane, builds one immutable candidate on a bounded background worker, revalidates the checkpoint before publication, and retries post-selection settlement without blocking Raft progress.", []Reference{
			ref("internal/raftmember/generation_driver.go", "ConfigureWALGeneration"), ref("internal/raftmember/generation_driver_test.go", "TestRuntimeWALGenerationBuildDoesNotBlockRaftProgress"),
		}},
		Shipped: Stage{StatusYes, "serve-rf3 enables checkpoint-driven WAL generation maintenance on a fixed logical cadence and settles an authenticated selecting generation before runtime adoption after restart.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "rf3WALGenerationIntervalTicks"), ref("internal/raftmember/generation_driver.go", "ConfigureWALGeneration"),
		}},
		Qualification: Stage{StatusPartial, "Repeated generation, idle, restart, selected-generation recovery, and blocked-build progress tests exist. Long-running external crash loops and hard retained-WAL/live-data ratio gates remain absent.", []Reference{
			ref("internal/raftmember/generation_driver_test.go", "TestRuntimeWALGenerationDriverRepeatedCompactionAndRestart"),
		}},
	},
	{
		Name: "Raft linearizable and follower point reads",
		Primitive: Stage{StatusYes, "Leader reads use ReadIndex. Follower reads require explicit applied-position and serving fences.", []Reference{
			ref("internal/raftservice/owner.go", "ReadPoint"), ref("internal/multiraft/host.go", "ReadIndex"),
		}},
		Integrated: Stage{StatusYes, "Replicated table profiles bind a public table and one scalar string/number ordered placement key to exact routes. The gateway follows leaders, refreshes a definitely stale catalog route once, and can select a sufficiently applied follower for the explicit follower contract. Composite and tenant-path placement keys remain absent.", []Reference{
			ref("gateway/replicated_data_read.go", "ReplicatedDataReader"), ref("gateway/replicated_table.go", "ResolveReplicatedTableKey"),
		}},
		Shipped: Stage{StatusYes, "vibedb-gateway serves canonical point get requests through the authenticated RF3 native pool when it consumes the replicated catalog. Linearizable reads use ReadIndex. Monotonic follower reads require the exact RouteID and applied index. SQL reads have no RF3 fallback.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "handleConnPolicy"), ref("gateway/replicated_data_read.go", "Read"),
		}},
		Qualification: Stage{StatusPartial, "Command-boundary tests cover canonical decoding, typed results, authorization, no SQL fallback, coalesced catalog refresh, follower selection, route mismatch, serving fences, duplicate-release-safe aggregate response reservations, direct zero-allocation response streaming, and blocked-client write timeout. There is no external public-gateway partition or staleness-latency gate.", []Reference{
			ref("cmd/vibedb-gateway/data_handler_test.go", "TestHandleConnDataDispatchesRF3ReadWithoutSQLFallback"), ref("gateway/replicated_data_read_test.go", "TestReplicatedDataReaderLinearizableRefreshesNotLeader"),
		}},
	},
	{
		Name: "Learner promotion, removal, and leader transfer",
		Primitive: Stage{StatusYes, "ConfChange, learner roles, durable promotion evidence, removal, and leader transfer exist in the Raft runtime.", []Reference{
			ref("internal/multiraft/host.go", "ProposeConfChange"), ref("internal/multiraft/host.go", "TransferLeader"),
		}},
		Integrated: Stage{StatusYes, "The durable move controller owns learner add, snapshot/catch-up waits, promotion, conditional leader transfer, ownership publication, two catalog-drain fences, source removal, retirement, grant finalization, and restart resume. Certified failures across independent groups are admitted as one atomic catalog operation set before any learner action begins.", []Reference{
			ref("internal/rebalanceexec/executor.go", "ExecuteReplicaMove"), ref("internal/rebalanceexec/controller.go", "SubmitSet"), ref("internal/rebalanceexec/controller.go", "RunPass"),
		}},
		Shipped: Stage{StatusYes, "serve-rf3 exposes authenticated membership, observation, snapshot-source, ownership, and retirement control. vibedb-gateway starts the resumable move controller when a strict replica-control manifest is present.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-gateway/replica_health_controller.go", "startGatewayReplicaControllers"), ref("cmd/vibedb-gateway/serve.go", "runServe"),
		}},
		Qualification: Stage{StatusPartial, "Deterministic action tests cover the complete ordered lifecycle and real hosts cover authenticated transfer with continued apply. A mandatory Linux CI gate runs three physical voters, one shared-listener cold target, and one shipped gateway across two independent groups; it requires atomic move-set discovery, snapshot bootstrap, catch-up, promotion, catalog publication, source removal, controller SIGKILL/restart, certified cleanup, non-rejoin, and hard admission, p50/p99/max, network, storage, WAL, RSS, and cleanup bounds. The claim remains Partial until that new gate passes CI; failure replacement correctly retains the already-elected live leader, while planned live-source moves exercise conditional leader transfer separately.", []Reference{
			ref("internal/rebalanceexec/executor_test.go", "TestExecutorMapsExactMembershipSnapshotWaitAndDrainActions"), ref("internal/multiraft/host_leader_transfer_real_test.go", "TestThreeRealHostsTransferLeaderThroughAuthenticatedTransportAndContinueApply"), ref("cmd/vibedb-gateway/replica_replacement_process_test.go", "TestGatewayAutomaticReplicaReplacementProcesses"),
		}},
	},
	{
		Name: "Online snapshot artifact transfer",
		Primitive: Stage{StatusYes, "A bounded authenticated repository, resumable chunk protocol, descriptor identity, and artifact verification exist.", []Reference{
			ref("internal/snapshottransfer/repository.go", "Repository"), ref("internal/snapshottransfer/service.go", "Receiver"),
		}},
		Integrated: Stage{StatusYes, "Artifact production, authenticated transport, crash-safe empty-learner activation, exact-incarnation adoption, Multi-Raft host addition, catch-up observation, and later promotion are composed by the durable move controller.", []Reference{
			ref("internal/snapshottransfer/learner_install.go", "InstallPublishedLearner"), ref("internal/rebalanceexec/executor.go", "ExecuteReplicaMove"),
		}},
		Shipped: Stage{StatusYes, "serve-rf3 exposes authenticated bounded, group-scoped source-control and snapshot-data listeners. bootstrap-rf3 receives, verifies, installs, and resumes one or more cold learners through one physical NodeID/control listener before reopening every installed group through the ordinary serving command.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-shard/bootstrap_rf3.go", "bootstrapPreparedRF3"), ref("cmd/vibedb-shard/bootstrap_rf3_groups.go", "bootstrapPreparedRF3Groups"),
		}},
		Qualification: Stage{StatusPartial, "Resume, disconnect, corruption, bounds, TLS rotation, activation-seam fault settlement, post-Host-add rejection reopen, exact-incarnation retry, and chunk benchmarks exist. Target artifacts use a crash-safe authenticated publish-to-delete transition after learner certification. Completed source exports are released only after the durable target-install witness. Abandoned stages require a canonical catalog-RF3 cancellation witness with the exact operation, step, artifact, source owner epoch, expired lease revision, target incarnation, and schema/replica generations. A bounded gateway scheduler routes that witness to the exact source, and repository tests cover staged and published crash cuts, reopen, idempotent replay, slow-transfer protection, byte ceilings, and retained-byte failure. A Linux-only external multi-process gate now spans two group-scoped learner transfers over shared physical listeners, atomic catalog admission, real catalog cancellation, authenticated source routing, repository rename and unlink crash cuts, target/source/gateway restart, exact cleanup convergence, and hard latency, RSS, network, WAL, and storage bounds. CI requires three unskipped passes; qualification remains Partial until that gate passes there.", []Reference{
			ref("internal/snapshottransfer/source_provider_test.go", "TestRetainedSourceProviderExportsAndObservesAfterReopen"), ref("internal/snapshottransfer/source_control_test.go", "TestSourceControlClientRoutesExactReplicatedAbandonment"), ref("internal/snapshottransfer/abandonment_test.go", "TestRepositoryRecoversEveryAbandonmentNamespacePhase"), ref("internal/rebalanceexec/abandonment_test.go", "TestAbandonmentSchedulerCrashRestartAndByteGateDoNotSkipWitness"), ref("cmd/vibedb-gateway/replica_replacement_process_test.go", "TestGatewayAutomaticReplicaReplacementProcesses"), ref("internal/snapshottransfer/learner_install_test.go", "TestInstallPublishedLearnerRetriesExactIncarnationAfterHostBoundary"), ref("internal/snapshottransfer/transfer_test.go", "BenchmarkSnapshotServiceChunk"),
		}},
	},
	{
		Name: "Automatic split and replica-move execution",
		Primitive: Stage{StatusYes, "Durable split intent, source capture, immutable artifact construction, tail translation, ownership seal, child staging, RF3 readiness, catalog publication, retained pruning, move plans, failure authorization, and replica-move execution exist. Child image and global-index placement accumulators provide constant-size cut proofs without rescanning relations at cutover.", []Reference{
			ref("internal/splitcontroller/replicated_executor.go", "AdmitReplicatedPlan"), ref("internal/splitcontroller/local_source_actions.go", "LocalSourceActions"), ref("internal/splitcontroller/local_child_actions.go", "LocalChildActions"), ref("internal/rangesplit/stage_image.go", "childStageImageAccumulator"), ref("internal/replicatedstate/relation_placement_accumulator.go", "GlobalIndexPlacementProof"), ref("internal/rebalanceexec/executor.go", "ExecuteReplicaMove"),
		}},
		Integrated: Stage{StatusYes, "The catalog RF3 journal and shard-local durable runtime reconstruct source and child observations, exact action grants, plan admission, capture, artifact, stage, tail, seal, activation, publication, and retained pruning. The replica-move path composes failure evidence, candidate selection, membership grants, snapshot bootstrap, catch-up, promotion, catalog drains, removal, and cleanup.", []Reference{
			ref("internal/splitcontroller/local_observation_provider.go", "LocalPlanObservationProvider"), ref("internal/splitcontroller/composite_shard_executor.go", "CompositeShardActionExecutor"), ref("internal/splitcontroller/controller_service.go", "NewServingControllerService"), ref("internal/rebalanceexec/controller.go", "Controller"),
		}},
		Shipped: Stage{StatusPartial, "vibedb-gateway scans replicated split operation records and sends controller triggers. With a strict replica-control manifest it also publishes quorum health revisions, schedules certified failed-replica replacements, and resumes durable replica moves. Replica movement is command-composed. No public operator intake creates split operations, and serve-rf3 still passes nil split and plan-admission handlers to its control mux. Retained sources reject base and global-index mutations and point reads outside their post-cutover range.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-gateway/replica_health_controller.go", "startGatewayReplicaControllers"), ref("internal/replicatedstate/relation_bundle.go", "GlobalIndexProfile"),
		}},
		Qualification: Stage{StatusPartial, "Internal crash matrices cover replicated-journal recovery, durable source capture and seal, child stage and tail retry, publication-before-prune, and post-cutover ownership fencing. Tests prove quorum readiness across distinct replicas, O(1) child seal and activation handoff, and constant-size global-index ownership proof. The initial source partition remains one bounded scan, and crash recovery may explicitly audit the sealed physical image. No shipped route/action/publication composition gate, external split-under-load kill or partition gate, or range-scan routing proof exists.", []Reference{
			ref("internal/splitcontroller/local_source_actions_test.go", "TestLocalSourceActionsRecoverCaptureAndPublishImmutableArtifacts"), ref("internal/splitcontroller/local_source_seal_test.go", "TestLocalSourceSealAndCutoverCertificateSurviveRestart"), ref("internal/splitcontroller/reconcile_test.go", "TestChildActionsRequireMonotonicExactEvidence"), ref("internal/rangesplit/stage_image_incremental_test.go", "TestChildStageSealDoesNotScanRows"), ref("internal/splitcontroller/global_index_cut_test.go", "TestGlobalIndexCutUsesCanonicalUniqueAndNonUniquePlacementAtBoundary"), ref("internal/splitcontroller/execute_test.go", "TestPublishBeforePruneCrashMatrixNeverLosesOrDoubleRoutesRows"),
		}},
	},
	{
		Name: "Replicated catalog and distributed DDL",
		Primitive: Stage{StatusYes, "A dedicated RF3 catalog authority provides linearizable catalog heads and bounded resumable operation records. Exact schema rollout primitives prepare immutable shard bundles, bind per-group receipts, authorize one catalog cut, activate it, drain the prior generation, and support pre-activation abort.", []Reference{
			ref("gateway/replicated_catalog_authority.go", "ReplicatedCatalogAuthority"), ref("gateway/schema_rollout.go", "PrepareSchemaRollout"), ref("internal/schemainstall/installer.go", "Installer"),
		}},
		Integrated: Stage{StatusYes, "Catalog publication, topology journals, schema rollout records, shard installers, the authenticated shard control service, and the bounded gateway controller share exact catalog, group, relation-manifest, and contract digests.", []Reference{
			ref("gateway/schema_rollout_controller.go", "NewSchemaRolloutController"), ref("internal/schemainstall/control.go", "NewControlService"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-gateway/schema_rollout_admin.go", "executeGatewaySchemaRollout"),
		}},
		Shipped: Stage{StatusYes, "serve-rf3 installs the authenticated crash-safe schema control service. The experimental vibedb-gateway schema-rollout command consumes a strict canonical target and per-replica plan, gathers exact receipts with bounded concurrency, and conditionally publishes one catalog successor. It is not a general SQL DDL endpoint.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-gateway/main.go", "run"), ref("cmd/vibedb-gateway/schema_rollout_admin.go", "executeGatewaySchemaRollout"),
		}},
		Qualification: Stage{StatusPartial, "Catalog tests cover quorum publication and leader loss. Schema tests cover exact catalog activation, restart, pre-activation abort, mixed-old/new refusal, authenticated control, crash-safe installer reopen, strict command-plan validation, and an external-process leader-loss recovery cut. No rolling mixed-build or SQL DDL rollback gate exists.", []Reference{
			ref("gateway/schema_rollout_test.go", "TestSchemaRolloutPrepareActivateExactCatalog"), ref("gateway/schema_rollout_process_test.go", "TestSchemaRolloutExternalProcessLeaderLossAndMixedGenerationRecovery"), ref("internal/schemainstall/installer_test.go", "TestInstallerCrashReopenAuthorizationActivationAndDrain"), ref("cmd/vibedb-gateway/schema_rollout_admin_test.go", "TestGatewaySchemaRolloutManifestRequiresCanonicalVibeJSON"), ref("internal/raftservice/controlplane_catalog_rf3_test.go", "TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart"),
		}},
	},
	{
		Name: "Hot-shard detection and rebalancing",
		Primitive: Stage{StatusYes, "Bounded pressure selection, failure-domain placement, split planning, and replica-move selection exist.", []Reference{
			ref("internal/topologyscheduler/admission.go", "SelectSplits"), ref("internal/topologyscheduler/replica_move.go", "SelectReplicaMoves"),
		}},
		Integrated: Stage{StatusYes, "Routed requests feed bounded per-allocation recorders. A collector publishes canonical pressure cuts through catalog RF3. A clockless controller qualifies sustained pressure, selects either a split or replica move, and hands one idempotent admission to the existing operation journals.", []Reference{
			ref("internal/hotshard/collector.go", "Collector"), ref("internal/hotshard/controller.go", "Controller"), ref("internal/hotshard/operation_sink.go", "OperationSink"),
		}},
		Shipped: Stage{StatusPartial, "With -hot-shard-capacity, vibedb-gateway collects routed request pressure and periodically publishes bounded cuts. vibedb cluster dev provisions that capacity file. The command does not run RunReplicatedPass or OperationSink, so pressure does not yet trigger automatic split or replica-move admission.", []Reference{
			ref("cmd/vibedb-gateway/hot_shard_runtime.go", "gatewayHotShardRuntime"), ref("cmd/vibedb/cluster_dev.go", "ensureDevCluster"),
		}},
		Qualification: Stage{StatusPartial, "Tests cover deterministic sustained-hotness qualification, logical cooldown, skew refusal, failure domains, capacity bounds, fixed-memory planning, and command configuration. No command-composed pressure-to-operation gate or sustained foreground-latency benchmark exists.", []Reference{
			ref("internal/hotshard/controller_test.go", "TestControllerQualifiesHotShardAndRetriesByteIdenticalAdmission"), ref("internal/hotshard/controller_test.go", "TestControllerClockSkewCannotAdvanceReplicatedEvidence"), ref("cmd/vibedb-gateway/hot_shard_runtime_test.go", "TestGatewayHotShardCapacityRequiresCanonicalBoundedFile"),
		}},
	},
}
