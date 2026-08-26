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
		Qualification: Stage{StatusPartial, "Identity, generation rotation, confused-deputy, SQL classification, connection, and allocation tests exist. No network-chaos or authorization-throughput gate covers the shipped commands.", []Reference{
			ref("gateway/client_tls_test.go", "TestClientTLSAuthenticatesAuthorizesRotatesAndSeparatesALPN"), ref("shardservice/authorization_test.go", "TestShardAuthorizationRejectsConfusedDeputyAndSeparatesRoles"),
		}},
	},
	{
		Name: "Coherent multi-shard reads",
		Primitive: Stage{StatusYes, "Bounded read-fence acquisition and release establish a scoped vector cut across selected static shards. There is no common cross-group RF3 read snapshot.", []Reference{
			ref("gateway/read_snapshot.go", "snapshotFanout"), ref("shardservice/read_fence.go", "readFenceSet"),
		}},
		Integrated: Stage{StatusYes, "The static gateway read path acquires fences before fanout and refuses partial results.", []Reference{
			ref("gateway/e2e_test.go", "TestE2EFanoutShapes"), ref("shardservice/acceptance_test.go", "TestAcceptanceReadSnapshotPinned"),
		}},
		Shipped: Stage{StatusYes, "The query operation uses this path. It does not claim a global MVCC timestamp or cluster-wide linearizability.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "execRequest"),
		}},
		Qualification: Stage{StatusPartial, "Lease, cleanup, stale refusal, and pinned-snapshot tests exist. There is no partition or long-running tail-latency gate.", []Reference{
			ref("shardservice/read_fence_test.go", "TestReadFenceLeaseExpiresAndWakesWriter"), ref("shardservice/acceptance_test.go", "TestAcceptanceReadSnapshotPinned"),
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
		Integrated: Stage{StatusYes, "The public RF3 gateway orchestrator drives fused commands through the leader-aware executor, retains exact outcome-unknown bytes, and recovers from replicated ReadIndex witnesses without consulting the legacy journal. Request replay runs before catalog pinning and reuses the first execution's generation and shard metadata instead of replanning against changed topology.", []Reference{
			ref("gateway/replicated_transaction.go", "NewReplicatedTransactionOrchestrator"), ref("gateway/replicated_transaction_recover.go", "Recover"), ref("gateway/replicated_request_registry.go", "Replay"),
		}},
		Shipped: Stage{StatusYes, "vibedb-gateway lowers one exec_batch over one or more RF3 groups into strict whole-document INSERT, exact-primary-key whole-document UPDATE, and exact-primary-key DELETE relation mutations. Same-group multi-statement and multi-relation batches remain atomic. The registry key is stable authenticated node scope plus caller request ID, excluding policy generation; its digest covers exact ordered SQL, class, and typed parameter bytes without routes or catalog generation. Typed responses return transaction identity, committed, and outcome-unknown fields.", []Reference{
			ref("gateway/transaction.go", "ExecBatchRequest"), ref("cmd/vibedb-gateway/serve.go", "execRequest"),
		}},
		Qualification: Stage{StatusPartial, "State-machine and schedule gates cover atomic transitions, reclamation, exact retries, and a 2P+1 decision/apply proposal schedule without a participant-count contract. The normal success path adds one retirement proposal and trusts the P route-fenced applied completions instead of issuing a redundant P-wide ReadIndex wave; recovery and ambiguous cleanup still prove terminal state with leader ReadIndex. One real RF3 gate drives the shipped executor across two independently led groups and a same-group multi-statement batch. It proves SQL lowering, hidden-commit recovery after source isolation, terminal retry caching, replica convergence, correct group routing, former-leader refusal, and single-participant atomic execution. Registry tests prove that unproved pre-admission failures are shared with current waiters and then removed rather than retained terminally. Command-envelope tests prove the typed response. External-process, exhaustive crash-cut, and sustained performance gates remain absent.", []Reference{
			ref("internal/replication/transaction_perf_contract_test.go", "TestReplicatedTransactionEncodedSchedulePerformanceTargets"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestShippedExecBatchLowersAndRecoversAcrossTwoRealRF3Groups"), ref("gateway/replicated_request_registry_test.go", "TestReplicatedTransactionRequestRegistryDoesNotCacheUnprovedPreAdmissionError"), ref("cmd/vibedb-gateway/transaction_response_test.go", "TestWriteServeResponseCarriesTypedRF3TransactionOutcome"),
		}},
	},
	{
		Name: "RF3 transaction recovery reads",
		Primitive: Stage{StatusYes, "A closed hidden-state reader provides exact coordinator and participant lookup, paged manifest access, and bounded resumable active-coordinator scans without a participant-count contract.", []Reference{
			ref("internal/replicatedstate/transaction_recovery_read.go", "TransactionRecoveryReadInto"), ref("internal/replicatedstate/transaction_recovery_read.go", "TransactionRecoveryReadRequest"),
		}},
		Integrated: Stage{StatusYes, "The dedicated transaction-recovery capability, leader-only ReadIndex path, native shard protocol, and leader-aware gateway executor share exact byte, row, applied-index, and serving-fence bounds. The RF3 transaction orchestrator consumes those reads for coordinator, manifest, participant, and terminal proofs.", []Reference{
			ref("gateway/replicated_transaction_recovery.go", "ReadTransactionRecovery"), ref("gateway/replicated_transaction_recover.go", "Recover"),
		}},
		Shipped: Stage{StatusYes, "vibedb-shard serve-rf3 installs the authenticated recovery reader. vibedb-gateway constructs the RF3 orchestrator plus a bounded same-process request registry and redrives retained outcome-unknown handles every five seconds. Executing, pending, and terminal retries replay before catalog pinning under stable authenticated-node scope, so catalog generation, split, or move does not replan an admitted request. The command performs no automatic terminal expiry, exposes no client ACK or expiry, and never calls the registry's scoped Forget API: 65,536 retained entries backpressure new RF3 writes. An embedding may call Forget only after it has an application-level acknowledgement that the terminal result no longer needs retry protection. A durable replicated ledger or safe explicit client ACK is required before shipped reclamation.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("cmd/vibedb-gateway/serve.go", "startGatewayRecovery"),
		}},
		Qualification: Stage{StatusPartial, "Real RF3 tests prove leader-only recovery, replacement-leader continuity, isolated-former-leader refusal, and the shipped executor recovering a hidden committed result across two groups. Registry tests prove bounded capacity, duplicate coalescing, exact-handle ownership, terminal retention, and no lock held across I/O. Gateway-process restart, abort recovery, and the shipped periodic sweep remain unqualified.", []Reference{
			ref("internal/raftservice/owner_rf3_transaction_test.go", "TestRF3TransactionRecoveryReadIsLeaderOnlyAndSurvivesGatewayReplacement"), ref("internal/raftservice/owner_rf3_multigroup_transaction_test.go", "TestShippedExecBatchLowersAndRecoversAcrossTwoRealRF3Groups"), ref("gateway/replicated_request_registry_test.go", "TestReplicatedTransactionRequestRegistryRetainsUnknownAndRetryRecovers"),
		}},
	},
	{
		Name: "Global exact index routing and maintenance",
		Primitive: Stage{StatusYes, "Catalog metadata, independent index placement, exact lookup, lifecycle fencing, and write expansion exist.", []Reference{
			ref("gateway/index_metadata.go", "IndexMetadata"), ref("gateway/global_index.go", "GlobalIndexProgram"),
		}},
		Integrated: Stage{StatusYes, "Static planning can select a global index and static writes add index participants without forcing the base row onto that shard. RF3 exec_batch lowering does not yet emit global-index relation mutations.", []Reference{
			ref("gateway/global_index_test.go", "TestGlobalIndexRoutesIndexAndBaseIndependently"), ref("gateway/writer.go", "prepareGlobalIndexWrites"),
		}},
		Shipped: Stage{StatusYes, "The static gateway consumes ready global-index metadata from its catalog and the shard service exposes raw exact lookup.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runServe"), ref("shardservice/server_test.go", "TestServerGlobalIndexLookupUsesRawBoundedLane"),
		}},
		Qualification: Stage{StatusPartial, "Routing, lifecycle, byte preservation, rollback, and allocation tests exist. There is no horizontally scaled index churn or failure benchmark gate.", []Reference{
			ref("gateway/global_index_test.go", "TestGlobalIndexBuildAndDrainLifecyclesStayWriteMaintained"), ref("gateway/global_index_vibejson_test.go", "TestGlobalIndexFlatScalarRouteWarmAllocationFree"),
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
		Shipped: Stage{StatusYes, "vibedb-shard serve-rf3 opens an externally prepared exact replicated SQL/apply bundle. vibedb-gateway can lower exact-key mutations for multiple RF3 base-table relations into the same participant without table or SQL strings entering Raft.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"), ref("gateway/replicated_sql_transaction.go", "planReplicatedSQLTransaction"),
		}},
		Qualification: Stage{StatusPartial, "Deterministic apply, replay, malformed-command, failure-atomic, and multi-table SQL lowering tests exist. No shipped RF3 global-index mutation fault gate exists.", []Reference{
			ref("internal/replicatedstate/relation_bundle_test.go", "TestRelationBundleCheckpointCrashPhasesNeverRecoverSkew"), ref("gateway/replicated_sql_transaction_test.go", "TestReplicatedSQLTransactionLowersExactMultiTableMutationsByGroupAndRelation"),
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
		Shipped: Stage{StatusYes, "vibedb-shard serve-rf3 opens one externally prepared stable three-voter group, constructs its bounded Multi-Raft host and authenticated peer transport, and serves the authenticated native endpoint.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"),
		}},
		Qualification: Stage{StatusPartial, "A shipped-composition three-process gate proves retained-state opening, mutual TLS, natural election, authenticated reads, and clean process shutdown. Internal fault gates additionally prove follower catch-up, pre-admission leader loss, post-apply response loss, byte-identical retry, and acknowledged-result survival. Every quorum/apply cut, membership change, and snapshot bootstrap remain unqualified.", []Reference{
			ref("cmd/vibedb-shard/serve_rf3_process_test.go", "TestServeRF3ShippedCompositionThreeProcesses"), ref("internal/raftservice/process_rf3_test.go", "TestRF3NativeServingThreeProcessRecoveryEvidence"), ref("internal/raftservice/owner_rf3_test.go", "TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry"),
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
		Integrated: Stage{StatusPartial, "Authenticated internal hosts exercise the lifecycle, but no controller owns and resumes it as an operation.", []Reference{
			ref("internal/multiraft/host_leader_transfer_real_test.go", "TestThreeRealHostsTransferLeaderThroughAuthenticatedTransportAndContinueApply"),
		}},
		Shipped: Stage{StatusNo, "serve-rf3 requires one already-stable three-voter roster and rejects pending membership changes; no command provisions, promotes, removes, or transfers a member.", nil},
		Qualification: Stage{StatusPartial, "Deterministic real-host tests cover authenticated transfer and continued apply. External crash/restart coverage for the complete lifecycle is absent.", []Reference{
			ref("internal/multiraft/host_leader_transfer_real_test.go", "TestThreeRealHostsTransferLeaderThroughAuthenticatedTransportAndContinueApply"),
		}},
	},
	{
		Name: "Online snapshot artifact transfer",
		Primitive: Stage{StatusYes, "A bounded authenticated repository, resumable chunk protocol, descriptor identity, and artifact verification exist.", []Reference{
			ref("internal/snapshottransfer/repository.go", "Repository"), ref("internal/snapshottransfer/service.go", "Receiver"),
		}},
		Integrated: Stage{StatusPartial, "Artifact production, transport, crash-safe empty-learner activation, exact-incarnation adoption, and Multi-Raft host addition are integrated internally. Suffix catch-up and promotion remain explicit later barriers without a controller.", []Reference{
			ref("internal/snapshottransfer/learner_install.go", "InstallPublishedLearner"), ref("sql/driver/replicated_snapshot_stage.go", "ResumeReplicatedSnapshotActivation"),
		}},
		Shipped: Stage{StatusNo, "serve-rf3 opens prepared local artifacts but exposes no snapshot service, transfer, or learner bootstrap configuration.", nil},
		Qualification: Stage{StatusPartial, "Resume, disconnect, corruption, bounds, TLS rotation, activation-seam fault settlement, post-Host-add rejection reopen and exact-incarnation retry, and chunk benchmarks exist. Live suffix catch-up, promotion, and external multi-process crash gates remain absent.", []Reference{
			ref("internal/snapshottransfer/learner_install_test.go", "TestInstallPublishedLearnerRetriesExactIncarnationAfterHostBoundary"), ref("sql/driver/replicated_snapshot_stage_test.go", "TestReplicatedSnapshotStageSameHandleFaultSettlement"), ref("internal/snapshottransfer/transfer_test.go", "BenchmarkSnapshotServiceChunk"),
		}},
	},
	{
		Name: "Automatic split and replica-move execution",
		Primitive: Stage{StatusPartial, "Split staging, tail translation, placement, durable split execution, move plans, and safety reconcilers exist. Replica-move execution is still absent.", []Reference{
			ref("internal/rangesplit/stage.go", "ChildStage"), ref("internal/splitcontroller/replicated_executor.go", "ExecuteReplicatedStep"), ref("internal/rebalance/reconcile.go", "Reconcile"),
		}},
		Integrated: Stage{StatusPartial, "A dedicated catalog RF3 journal drives resumable publish-before-prune split actions through authenticated shard-control routes. Learner creation, catch-up, source removal, and replica moves are not orchestrated.", []Reference{
			ref("internal/splitcontroller/controller_service.go", "ControllerService"), ref("gateway/replicated_catalog_authority.go", "ReplicatedCatalogAuthority"),
		}},
		Shipped: Stage{StatusPartial, "vibedb-gateway runs the replicated split-controller loop when configured with the dedicated catalog RF3 authority, and serve-rf3 can host an externally prepared fixed group. No command provisions that group or executes replica moves.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "runSplitController"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"),
		}},
		Qualification: Stage{StatusPartial, "Publish-before-prune crash matrices, replicated-journal recovery, controller reconstruction, and a three-process catalog restart/leader-loss gate exist. Foreground-traffic and replica-move crash gates do not.", []Reference{
			ref("internal/splitcontroller/execute_test.go", "TestPublishBeforePruneCrashMatrixNeverLosesOrDoubleRoutesRows"), ref("internal/raftservice/controlplane_catalog_rf3_test.go", "TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart"),
		}},
	},
	{
		Name: "Replicated catalog and distributed DDL",
		Primitive: Stage{StatusPartial, "A dedicated RF3 catalog authority provides linearizable catalog heads and bounded resumable operation records. A distributed DDL rollout protocol is absent.", []Reference{
			ref("gateway/replicated_catalog_authority.go", "ReplicatedCatalogAuthority"), ref("internal/replicatedstate/relation_bundle.go", "RelationCollection"),
		}},
		Integrated: Stage{StatusPartial, "Catalog publication and split-operation authority share one dedicated, topology-authorized RF3 relation with exact catalog/controlplane placement and schema-generation fences. Membership and schema rollout are not controlled there yet.", []Reference{
			ref("gateway/replicated_catalog_document.go", "ReplicatedCatalogDistribution"), ref("internal/splitcontroller/replicated_executor.go", "ReplicatedOperationJournal"),
		}},
		Shipped: Stage{StatusPartial, "vibedb-gateway consumes the replicated catalog and refuses arbitrary catalog placement coordinates, while serve-rf3 can open an externally prepared fixed catalog group. No command bootstraps that group, and distributed DDL remains refused.", []Reference{
			ref("cmd/vibedb-gateway/serve.go", "newReplicatedCatalogGateway"), ref("cmd/vibedb-shard/serve_rf3.go", "servePreparedRF3"),
		}},
		Qualification: Stage{StatusPartial, "A three-process RF3 gate covers quorum publication, byte-identical replay, controller reconstruction, outcome-unknown settlement, generation CAS, and leader loss. Rolling schema compatibility and DDL rollback gates are absent.", []Reference{
			ref("internal/raftservice/controlplane_catalog_rf3_test.go", "TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart"),
		}},
	},
	{
		Name: "Hot-shard detection and rebalancing",
		Primitive: Stage{StatusYes, "Bounded pressure selection, failure-domain placement, split planning, and replica-move selection exist.", []Reference{
			ref("internal/topologyscheduler/admission.go", "SelectSplits"), ref("internal/topologyscheduler/replica_move.go", "SelectReplicaMoves"),
		}},
		Integrated: Stage{StatusPartial, "Schedulers produce fenced plans and reconcilers validate them, but no live controller consumes runtime metrics and executes the result.", []Reference{
			ref("internal/topologyscheduler/capacity_placement.go", "PlaceSplitDestinations"), ref("internal/rebalance/reconcile.go", "Reconcile"),
		}},
		Shipped: Stage{StatusNo, "The commands neither collect a cluster pressure view nor rebalance shards.", nil},
		Qualification: Stage{StatusPartial, "Determinism, failure-domain, capacity, and zero-allocation planner tests exist. No sustained hot-shard or foreground-latency benchmark gate exists.", []Reference{
			ref("internal/topologyscheduler/capacity_placement_test.go", "TestPlaceSplitDestinationsIsFixedSpaceAndWarmAllocationFree"), ref("internal/topologyscheduler/replica_move_test.go", "TestSelectReplicaMovesIsFixedSpaceAndWarmAllocationFree"),
		}},
	},
}
