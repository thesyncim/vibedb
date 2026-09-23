package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRF3CatalogGenesisOnlyFinishesForProvenSourceRetirement(t *testing.T) {
	for _, name := range []string{"authorized", "completed", "unproven", "wrong group", "wrong member", "wrong store", "wrong allocation", "different error", "closed journal"} {
		t.Run(name, func(t *testing.T) {
			journal, err := replicaaction.OpenFileJournal(t.TempDir(), 8)
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			record := rf3RetirementRecoveryRecord(rf3RecoveryEnrollmentIntent())
			if err := journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
				t.Fatal(err)
			}
			if name != "unproven" {
				record.Revision, record.State = 2, replicaaction.RetirementAuthorized
				if err := journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
					t.Fatal(err)
				}
			}
			if name == "completed" {
				record.Revision, record.State = 3, replicaaction.Complete
				if err := journal.PublishReplicaAction(t.Context(), 2, record); err != nil {
					t.Fatal(err)
				}
			}
			fence := record.Request.Fence
			identity := raftmember.RuntimeIdentity{Group: fence.Group, MemberID: fence.MemberID,
				StoreID: fence.StoreID, AllocationGeneration: fence.AllocationGeneration,
				NodeIncarnation: fence.NodeIncarnation + 1}
			cause := multiraft.ErrGroupNotFound
			switch name {
			case "wrong group":
				identity.Group.GroupID[0]++
			case "wrong member":
				identity.MemberID++
			case "wrong store":
				identity.StoreID[0]++
			case "wrong allocation":
				identity.AllocationGeneration++
			case "different error":
				cause = errRF3CatalogGenesis
			case "closed journal":
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err = finishRF3CatalogGenesis(t.Context(), journal, identity, cause)
			if name == "authorized" || name == "completed" {
				if err != nil {
					t.Fatalf("proven retirement: %v", err)
				}
			} else if !errors.Is(err, cause) {
				t.Fatalf("initializer failure lost: got %v, want %v", err, cause)
			}
		})
	}
}

func TestInitializeRF3CatalogGenesisExistingLifecycleDoesNotOpenPrivateSession(t *testing.T) {
	for _, lifecycle := range []struct {
		name string
		want gateway.NodeLifecycle
	}{
		{name: "enforcing", want: gateway.NodeDraining},
		{name: "retired", want: gateway.NodeDecommissioned},
	} {
		t.Run(lifecycle.name, func(t *testing.T) {
			localNode := rafttransport.NodeID{1}
			identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
			startupCommand := state.Command
			// A follower may still be waiting for leadership when the running
			// cluster moves this group. Recovery uses the current owner fence.
			state.Command.ReplicaSetVersion++
			state.Command.OwnershipEpoch++
			state.Command.RoutingVersion++
			state.Command.RouteGeneration++
			cut := rf3CatalogGenesisExistingCutFixture(t, localNode, lifecycle.want)
			var probeCalls, relationCalls, cutCalls int
			var gotRoute gateway.FrontendDrainRuntimeCatalogRoute
			err := initializeRF3CatalogGenesisWithReaders(
				context.Background(), config, nil, localNode, identity, startupCommand,
				func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
					probeCalls++
					return state, nil
				},
				func(_ context.Context, fence raftservice.ServingFence, relation replication.RelationID) (bool, bool, int, error) {
					relationCalls++
					if fence != state.Fence() || relation != replication.RelationID(config.Relation) {
						t.Fatalf("existing-state read fence=%+v relation=%d", fence, relation)
					}
					return true, true, 3, nil
				},
				func(_ context.Context, route gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
					cutCalls++
					gotRoute = route
					return cut, nil
				},
			)
			if err != nil {
				t.Fatalf("existing %s cut was not recovered: %v", lifecycle.name, err)
			}
			if probeCalls != 1 || relationCalls != 1 || cutCalls != 1 {
				t.Fatalf("existing %s calls probe=%d relation=%d cut=%d", lifecycle.name, probeCalls, relationCalls, cutCalls)
			}
			if gotRoute.Command != state.Command || gotRoute.Group != identity.Group ||
				gotRoute.AllocationGeneration != identity.AllocationGeneration ||
				gotRoute.Relation != replication.RelationID(config.Relation) {
				t.Fatalf("existing %s route=%+v state=%+v identity=%+v", lifecycle.name, gotRoute, state, identity)
			}
			if gotRoute.Command.ReplicaSetVersion == 1 || identity.NodeIncarnation == config.NodeIncarnation {
				t.Fatalf("fixture did not separate current owner runtime from physical plan: route=%+v identity-incarnation=%d plan-incarnation=%d", gotRoute, identity.NodeIncarnation, config.NodeIncarnation)
			}
			// The caller intentionally provides no owners and an absent plan. A
			// successful result proves the actual initialized-state orchestration
			// returned before plan loading, session Open, or SubmitOwnedAuthorized.
		})
	}
}

func TestInitializeRF3CatalogGenesisExistingRejectsMalformedCommittedCut(t *testing.T) {
	localNode := rafttransport.NodeID{1}
	identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
	var cutCalls int
	err := initializeRF3CatalogGenesisWithReaders(
		context.Background(), config, nil, localNode, identity, state.Command,
		func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
			return state, nil
		},
		func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
			return true, true, 1, nil
		},
		func(context.Context, gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
			cutCalls++
			return gateway.FrontendDrainRuntimeCut{}, nil
		},
	)
	if !errors.Is(err, errRF3CatalogGenesis) || cutCalls != 1 {
		t.Fatalf("malformed existing cut err=%v calls=%d", err, cutCalls)
	}
}

func TestInitializeRF3CatalogGenesisRejectsUnsafeCommandChanges(t *testing.T) {
	for name, change := range map[string]func(*raftservice.CommandFence){
		"replica regression": func(command *raftservice.CommandFence) { command.ReplicaSetVersion-- },
		"policy":             func(command *raftservice.CommandFence) { command.ActivePolicyGeneration++ },
		"protection":         func(command *raftservice.CommandFence) { command.ProtectionEpoch++ },
		"schema":             func(command *raftservice.CommandFence) { command.SchemaGeneration++ },
		"manifest":           func(command *raftservice.CommandFence) { command.RelationManifestDigest[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			localNode := rafttransport.NodeID{1}
			identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
			startupCommand := state.Command
			change(&state.Command)
			err := initializeRF3CatalogGenesisWithReaders(t.Context(), config, nil, localNode, identity, startupCommand,
				func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) { return state, nil },
				func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
					t.Fatal("unsafe command reached the catalog read")
					return false, false, 0, nil
				},
				func(context.Context, gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
					t.Fatal("unsafe command reached the committed cut")
					return gateway.FrontendDrainRuntimeCut{}, nil
				})
			if !errors.Is(err, errRF3CatalogGenesis) {
				t.Fatalf("unsafe command error=%v", err)
			}
		})
	}
}

func TestInitializeRF3CatalogGenesisRetriesPendingJournalAcrossLeaderFenceChange(t *testing.T) {
	localNode := rafttransport.NodeID{1}
	identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
	config.SessionJournal = filepath.Join(t.TempDir(), "catalog-genesis.session")
	inputs := rf3CatalogGenesisEmptySessionInputs(t, localNode, identity, state, config)
	owner := &rf3CatalogGenesisOwnerFixture{state: state, unknownKind: replication.CommandSessionOpen, unknownRemaining: 3}
	probe := func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
		return owner.state, nil
	}
	readRelation := func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
		return true, true, 0, nil
	}
	readCut := func(context.Context, gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
		return gateway.FrontendDrainRuntimeCut{}, errRF3CatalogGenesis
	}
	loadInputs := func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
		return inputs, nil
	}
	firstErr := initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	)
	if !errors.Is(firstErr, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("initial session outcome=%v, want outcome unknown", firstErr)
	}
	pending := owner.commands[0]
	if len(pending) == 0 || len(owner.commands) != 3 {
		t.Fatalf("pending open command count=%d bytes=%d commands=%x", len(owner.commands), len(pending), owner.commands)
	}
	present, err := gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || !present {
		t.Fatalf("pending private journal present=%t err=%v", present, err)
	}

	// A new owner term must settle the retained command byte-for-byte. The
	// initializer must then skip a second mutation after the pending Open is
	// settled; otherwise a retry could publish the genesis batch twice.
	owner.state.Status.Term++
	owner.unknownKind = 0
	owner.unknownRemaining = 0
	owner.commands = nil
	owner.fences = nil
	if err = initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	); err != nil {
		t.Fatalf("recovered private session=%v commands=%v fences=%v", err, rf3CatalogGenesisCommandKinds(owner.commands), owner.fences)
	}
	if len(owner.commands) < 4 || !bytes.Equal(owner.commands[0], pending) {
		t.Fatalf("recovery did not replay exact Open command: commands=%d exact=%t", len(owner.commands), len(owner.commands) > 0 && bytes.Equal(owner.commands[0], pending))
	}
	if owner.fences[0].Term != owner.state.Status.Term || owner.fences[0].Term == state.Status.Term {
		t.Fatalf("replayed Open used term=%d owner-current=%d original=%d", owner.fences[0].Term, owner.state.Status.Term, state.Status.Term)
	}
	counts := make(map[replication.CommandKind]int)
	for _, raw := range owner.commands {
		view, openErr := replication.OpenCommand(raw)
		if openErr != nil {
			t.Fatal(openErr)
		}
		counts[view.Kind()]++
	}
	if counts[replication.CommandSessionOpen] != 1 || counts[replication.CommandMutationBatch] != 1 ||
		counts[replication.CommandSessionRetire] != 1 || counts[replication.CommandSessionRelease] != 1 {
		t.Fatalf("recovery command sequence=%v commands=%d", counts, len(owner.commands))
	}
	present, err = gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || present {
		t.Fatalf("settled private journal present=%t err=%v", present, err)
	}
}

func TestInitializeRF3CatalogGenesisRetriesPendingMutationWithoutDuplicate(t *testing.T) {
	localNode := rafttransport.NodeID{1}
	identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
	config.SessionJournal = filepath.Join(t.TempDir(), "catalog-genesis.session")
	inputs := rf3CatalogGenesisEmptySessionInputs(t, localNode, identity, state, config)
	owner := &rf3CatalogGenesisOwnerFixture{state: state, unknownKind: replication.CommandMutationBatch, unknownRemaining: 3}
	probe := func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
		return owner.state, nil
	}
	readRelation := func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
		return true, true, 0, nil
	}
	readCut := func(context.Context, gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
		return gateway.FrontendDrainRuntimeCut{}, errRF3CatalogGenesis
	}
	loadInputs := func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
		return inputs, nil
	}
	if err := initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	); !errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("initial mutation outcome=%v, want outcome unknown commands=%v remaining=%d", err, rf3CatalogGenesisCommandKinds(owner.commands), owner.unknownRemaining)
	}
	if len(owner.commands) != 4 {
		t.Fatalf("initial open+mutation attempts=%v", rf3CatalogGenesisCommandKinds(owner.commands))
	}
	pending := owner.commands[len(owner.commands)-1]
	present, err := gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || !present {
		t.Fatalf("pending mutation journal present=%t err=%v", present, err)
	}

	owner.state.Status.Term++
	owner.unknownKind = 0
	owner.unknownRemaining = 0
	owner.commands = nil
	owner.fences = nil
	if err = initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	); err != nil {
		t.Fatalf("recovered mutation=%v commands=%v", err, rf3CatalogGenesisCommandKinds(owner.commands))
	}
	if len(owner.commands) != 3 || !bytes.Equal(owner.commands[0], pending) {
		t.Fatalf("mutation retry sequence=%v exact=%t", rf3CatalogGenesisCommandKinds(owner.commands), len(owner.commands) > 0 && bytes.Equal(owner.commands[0], pending))
	}
	if kinds := rf3CatalogGenesisCommandKinds(owner.commands); kinds[0] != replication.CommandMutationBatch ||
		kinds[1] != replication.CommandSessionRetire || kinds[2] != replication.CommandSessionRelease {
		t.Fatalf("recovery replayed unexpected commands=%v", kinds)
	}
	if owner.fences[0].Term != owner.state.Status.Term {
		t.Fatalf("mutation retry used term=%d current=%d", owner.fences[0].Term, owner.state.Status.Term)
	}
	present, err = gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || present {
		t.Fatalf("settled mutation journal present=%t err=%v", present, err)
	}
}

func TestInitializeRF3CatalogGenesisRecoversNonemptyRelationBeforePrivateCleanup(t *testing.T) {
	localNode := rafttransport.NodeID{1}
	identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
	config.SessionJournal = filepath.Join(t.TempDir(), "catalog-genesis.session")
	inputs := rf3CatalogGenesisEmptySessionInputs(t, localNode, identity, state, config)
	cut := rf3CatalogGenesisExistingCutFixture(t, localNode, gateway.NodeDraining)
	rows := 0
	owner := &rf3CatalogGenesisOwnerFixture{state: state, unknownKind: replication.CommandMutationBatch, unknownRemaining: 3}
	owner.onOutcomeUnknown = func(view replication.CommandView) {
		if view.Kind() != replication.CommandMutationBatch {
			t.Fatalf("outcome-unknown callback for %v", view.Kind())
		}
		// Model the real boundary: the mutation is committed and visible before
		// the client loses its response, so the next startup relation read is
		// nonempty while the exact private mutation remains in the journal.
		if rows == 0 {
			rows = 3
			owner.state.Status.Applied++
			owner.state.Status.Commit = owner.state.Status.Applied
		}
	}
	probe := func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
		return owner.state, nil
	}
	readRelation := func(_ context.Context, _ raftservice.ServingFence, _ replication.RelationID) (bool, bool, int, error) {
		return true, true, rows, nil
	}
	readCut := func(_ context.Context, route gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
		if route.Command != owner.state.Command {
			t.Fatalf("cleanup route command=%+v state=%+v", route.Command, owner.state.Command)
		}
		return cut, nil
	}
	loadCalls := 0
	loadInputs := func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
		loadCalls++
		return inputs, nil
	}
	firstErr := initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	)
	if !errors.Is(firstErr, raftservice.ErrOutcomeUnknown) || rows != 3 {
		t.Fatalf("committed mutation outcome=%v rows=%d commands=%v", firstErr, rows, rf3CatalogGenesisCommandKinds(owner.commands))
	}
	if loadCalls != 1 || len(owner.commands) != 4 {
		t.Fatalf("initial genesis commands=%v loadCalls=%d", rf3CatalogGenesisCommandKinds(owner.commands), loadCalls)
	}
	pending := owner.commands[len(owner.commands)-1]
	present, err := gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || !present {
		t.Fatalf("pending journal present=%t err=%v", present, err)
	}

	owner.state.Status.Term++
	owner.unknownKind = 0
	owner.unknownRemaining = 0
	owner.commands = nil
	owner.fences = nil
	if err = initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
	); err != nil {
		t.Fatalf("nonempty journal recovery=%v commands=%v", err, rf3CatalogGenesisCommandKinds(owner.commands))
	}
	if loadCalls != 2 || len(owner.commands) != 3 || !bytes.Equal(owner.commands[0], pending) {
		t.Fatalf("recovery commands=%v loadCalls=%d exactPending=%t", rf3CatalogGenesisCommandKinds(owner.commands), loadCalls, len(owner.commands) > 0 && bytes.Equal(owner.commands[0], pending))
	}
	want := []replication.CommandKind{replication.CommandMutationBatch, replication.CommandSessionRetire, replication.CommandSessionRelease}
	got := rf3CatalogGenesisCommandKinds(owner.commands)
	if len(got) != len(want) {
		t.Fatalf("recovery command kinds=%v want=%v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("recovery command kind[%d]=%v want=%v all=%v", index, got[index], want[index], got)
		}
	}
	present, err = gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil || present {
		t.Fatalf("recovery journal present=%t err=%v", present, err)
	}
}

func TestInitializeRF3CatalogGenesisRetriesPendingLifecycleExactly(t *testing.T) {
	for _, unknownKind := range []replication.CommandKind{
		replication.CommandSessionRetire, replication.CommandSessionRelease,
	} {
		t.Run(fmt.Sprintf("kind-%d", unknownKind), func(t *testing.T) {
			localNode := rafttransport.NodeID{1}
			identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
			config.SessionJournal = filepath.Join(t.TempDir(), "catalog-genesis.session")
			inputs := rf3CatalogGenesisEmptySessionInputs(t, localNode, identity, state, config)
			rows := 0
			owner := &rf3CatalogGenesisOwnerFixture{state: state, unknownKind: unknownKind, unknownRemaining: 3}
			owner.onOutcomeUnknown = func(view replication.CommandView) {
				if view.Kind() != unknownKind {
					t.Fatalf("outcome-unknown callback kind=%v want=%v", view.Kind(), unknownKind)
				}
				// Model the actual crash boundary: the private lifecycle command
				// is committed and the relation is already nonempty when the
				// response is lost. Recovery must inspect this state before
				// deciding whether it may settle the retained journal.
				rows = 3
				owner.state.Status.Applied++
				owner.state.Status.Commit = owner.state.Status.Applied
			}
			cut := rf3CatalogGenesisExistingCutFixture(t, localNode, gateway.NodeDecommissioned)
			probe := func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
				return owner.state, nil
			}
			readRelation := func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
				return true, true, rows, nil
			}
			readCut := func(_ context.Context, route gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
				if route.Command != owner.state.Command {
					t.Fatalf("lifecycle cleanup route=%+v state=%+v", route, owner.state)
				}
				return cut, nil
			}
			loadInputs := func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
				return inputs, nil
			}
			if err := initializeRF3CatalogGenesisWithDependencies(
				context.Background(), config, owner, localNode, identity, state.Command,
				probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
			); !errors.Is(err, raftservice.ErrOutcomeUnknown) {
				t.Fatalf("initial %v outcome=%v commands=%v", unknownKind, err, rf3CatalogGenesisCommandKinds(owner.commands))
			}
			if rows != 3 {
				t.Fatalf("initial %v lost-response fixture stayed empty", unknownKind)
			}
			pending := owner.commands[len(owner.commands)-1]
			owner.state.Status.Term++
			owner.unknownKind = 0
			owner.unknownRemaining = 0
			owner.commands = nil
			owner.fences = nil
			if err := initializeRF3CatalogGenesisWithDependencies(
				context.Background(), config, owner, localNode, identity, state.Command,
				probe, readRelation, readCut, loadInputs, initializeRF3CatalogGenesisSession,
			); err != nil {
				t.Fatalf("recovered %v lifecycle=%v commands=%v", unknownKind, err, rf3CatalogGenesisCommandKinds(owner.commands))
			}
			kinds := rf3CatalogGenesisCommandKinds(owner.commands)
			want := []replication.CommandKind{unknownKind}
			if unknownKind == replication.CommandSessionRetire {
				want = append(want, replication.CommandSessionRelease)
			}
			if len(kinds) != len(want) || !bytes.Equal(owner.commands[0], pending) {
				t.Fatalf("lifecycle retry kinds=%v want=%v exact=%t", kinds, want, len(owner.commands) > 0 && bytes.Equal(owner.commands[0], pending))
			}
			for index := range want {
				if kinds[index] != want[index] {
					t.Fatalf("lifecycle retry command[%d]=%v want=%v all=%v", index, kinds[index], want[index], kinds)
				}
			}
			present, err := gateway.NativeSessionJournalPresent(config.SessionJournal)
			if err != nil || present {
				t.Fatalf("settled lifecycle journal present=%t err=%v", present, err)
			}
		})
	}
}

func TestRF3CatalogGenesisRetryableTransientOutcomes(t *testing.T) {
	for _, err := range []error{
		raftservice.ErrOutcomeUnknown,
		errors.Join(fmt.Errorf("private command: %w", raftservice.ErrOutcomeUnknown), errRF3CatalogGenesis),
		raftmodel.ErrReadLeadershipLost,
		fmt.Errorf("existing catalog read: %w", raftmodel.ErrReadLeadershipLost),
		// An election during session retirement: the local owner refused the
		// submission, so the session reports it was not admitted.
		fmt.Errorf("rf3 catalog genesis session retire: local owner: %w: %w",
			raftmodel.ErrNotLeader, errors.New("replicated command was not admitted by this invocation")),
	} {
		if !rf3CatalogGenesisRetryable(err) {
			t.Fatalf("transient error %v was not retryable", err)
		}
	}
	for _, err := range []error{errRF3CatalogGenesis, context.Canceled, raftservice.ErrServingFence} {
		if rf3CatalogGenesisRetryable(err) {
			t.Fatalf("terminal error %v became retryable", err)
		}
	}
}

func rf3CatalogGenesisCommandKinds(commands [][]byte) []replication.CommandKind {
	kinds := make([]replication.CommandKind, 0, len(commands))
	for _, raw := range commands {
		view, err := replication.OpenCommand(raw)
		if err != nil {
			continue
		}
		kinds = append(kinds, view.Kind())
	}
	return kinds
}

func TestInitializeRF3CatalogGenesisRejectsNonLeaderBeforePlanLoad(t *testing.T) {
	localNode := rafttransport.NodeID{1}
	identity, state, config := rf3CatalogGenesisExistingFixture(t, localNode)
	nonLeader := state
	nonLeader.Status.LeaderID = 2
	nonLeader.Command.ReplicaSetVersion++
	owner := &rf3CatalogGenesisOwnerFixture{state: nonLeader}
	var loadCalls, sessionCalls int
	err := initializeRF3CatalogGenesisWithDependencies(
		context.Background(), config, owner, localNode, identity, state.Command,
		func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
			return owner.state, nil
		},
		func(context.Context, raftservice.ServingFence, replication.RelationID) (bool, bool, int, error) {
			return true, true, 0, nil
		},
		func(context.Context, gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
			return gateway.FrontendDrainRuntimeCut{}, errRF3CatalogGenesis
		},
		func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
			loadCalls++
			return rf3CatalogGenesisInputs{}, nil
		},
		func(context.Context, *rf3CatalogGenesisConfig, rf3CatalogGenesisInputs, rf3CatalogGenesisOwners, rafttransport.NodeID, raftmember.RuntimeIdentity, raftservice.ServingState) error {
			sessionCalls++
			return nil
		},
	)
	if !errors.Is(err, errRF3CatalogGenesisNotLeader) || loadCalls != 0 || sessionCalls != 0 {
		t.Fatalf("nonleader err=%v plan-loads=%d session-runs=%d", err, loadCalls, sessionCalls)
	}
}

type rf3CatalogGenesisOwnerFixture struct {
	state            raftservice.ServingState
	unknownKind      replication.CommandKind
	unknownRemaining int
	onOutcomeUnknown func(replication.CommandView)
	commands         [][]byte
	fences           []raftservice.ServingFence
	sequence         uint64
}

func (owner *rf3CatalogGenesisOwnerFixture) Probe(
	context.Context, raftmember.GroupKey,
) (raftservice.ServingState, error) {
	if owner == nil {
		return raftservice.ServingState{}, errRF3CatalogGenesis
	}
	return owner.state, nil
}

func (owner *rf3CatalogGenesisOwnerFixture) SubmitOwnedAuthorized(
	_ context.Context,
	fence raftservice.ServingFence,
	command []byte,
	authorize raftservice.ProposalAuthorization,
) (raftservice.Result, error) {
	if owner == nil || fence != owner.state.Fence() || authorize == nil || !authorize(owner.state) {
		return raftservice.Result{}, raftservice.ErrServingFence
	}
	view, err := replication.OpenCommand(command)
	if err != nil {
		return raftservice.Result{}, err
	}
	owned := append([]byte(nil), command...)
	owner.commands = append(owner.commands, owned)
	owner.fences = append(owner.fences, fence)
	if owner.unknownRemaining > 0 && owner.unknownKind == view.Kind() {
		owner.unknownRemaining--
		if owner.onOutcomeUnknown != nil {
			owner.onOutcomeUnknown(view)
		}
		return raftservice.Result{}, raftservice.ErrOutcomeUnknown
	}
	owner.sequence++
	applied := owner.state.Status.Applied + 1
	if applied < owner.sequence {
		applied = owner.sequence
	}
	owner.state.Status.Applied = applied
	if owner.state.Status.Commit < applied {
		owner.state.Status.Commit = applied
	}
	if view.Kind() == replication.CommandSessionRelease {
		return raftservice.Result{
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased, AppliedIndex: applied},
			State:   owner.state,
		}, replicatedstate.ErrSessionReleased
	}
	resultCode := replicatedstate.ResultApplied
	clientEpoch := view.ClientEpoch
	var result []byte
	switch view.Kind() {
	case replication.CommandSessionOpen:
		resultCode = replicatedstate.ResultSessionOpened
		clientEpoch = applied
	case replication.CommandSessionRetire:
		resultCode = replicatedstate.ResultSessionRetired
	case replication.CommandSessionRenew:
		resultCode = replicatedstate.ResultSessionRenewed
	}
	if resultCode == replicatedstate.ResultApplied {
		result, err = replicatedstate.AppendMutationCompletionResult(nil, resultCode, int64(view.MutationCount()))
		if err != nil {
			return raftservice.Result{}, err
		}
	}
	digest := replication.CompletionResultDigest(resultCode, replicatedstate.ResultFormatMutation, result)
	encoded, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID: view.ClusterID, ClusterIncarnation: view.ClusterIncarnation,
		TopologyRecoveryEpoch: view.TopologyRecoveryEpoch, Distribution: string(view.Distribution), Shard: string(view.Shard),
		AllocationGeneration: view.AllocationGeneration, ShardIncarnation: view.ShardIncarnation, GroupID: view.GroupID,
		ReplicaSetVersion: view.ReplicaSetVersion, ActivePolicyGeneration: view.ActivePolicyGeneration,
		ProtectionEpoch: view.ProtectionEpoch, RoutingVersion: view.RoutingVersion, RouteGeneration: view.RouteGeneration,
		Tenant: view.Tenant, ClientID: view.ClientID, ClientEpoch: clientEpoch, ClientSequence: view.ClientSequence,
		Fingerprint: view.Fingerprint, RetryHome: view.RetryHome, AppliedSequence: applied,
		ResultCode: resultCode, ResultFormat: replicatedstate.ResultFormatMutation,
		Storage: replication.CompletionInline, ResultLength: uint64(len(result)), ResultDigest: digest, InlineResult: result,
	})
	if err != nil {
		return raftservice.Result{}, err
	}
	return raftservice.Result{
		Outcome:    raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: applied, CompletionAppliedSequence: applied, CompletionBytes: len(encoded)},
		Completion: encoded, State: owner.state,
	}, nil
}

func rf3CatalogGenesisEmptySessionInputs(
	t testing.TB,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	state raftservice.ServingState,
	config *rf3CatalogGenesisConfig,
) rf3CatalogGenesisInputs {
	t.Helper()
	return rf3CatalogGenesisInputs{
		Snapshot: &gateway.Snapshot{},
		Route: gateway.ReplicatedRoute{
			Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard,
			Group: identity.Group, AllocationGeneration: identity.AllocationGeneration, Command: state.Command,
			RangeIdentity: replication.Digest{0xc1}, LineageDigest: replication.Digest{0xc2}, ForwardingRuleDigest: replication.Digest{0xc3},
			Replicas: []gateway.ReplicatedEndpoint{{
				Member: identity.MemberID, Node: localNode, StoreID: identity.StoreID,
				NodeIncarnation: config.NodeIncarnation, NativeEndpoint: "native-local", Address: "127.0.0.1:9101",
			}},
		},
		Mutations: []gateway.NativeMutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte("genesis"), Value: []byte(`{"id":1}`),
		}},
	}
}

func rf3CatalogGenesisExistingFixture(
	t testing.TB, localNode rafttransport.NodeID,
) (raftmember.RuntimeIdentity, raftservice.ServingState, *rf3CatalogGenesisConfig) {
	t.Helper()
	group := raftmember.GroupKey{
		ClusterID: [16]byte{0x11}, ClusterIncarnation: [16]byte{0x22},
		TopologyRecoveryEpoch: 7, ShardIncarnation: [16]byte{0x33}, GroupID: [16]byte{0x44},
	}
	identity := raftmember.RuntimeIdentity{
		Group: group, Distribution: string(gateway.ReplicatedCatalogDistribution),
		Shard: string(gateway.ReplicatedCatalogShard), AllocationGeneration: 3,
		MemberID: 1, StoreID: [16]byte{0x55}, NodeIncarnation: 2,
		RelationManifestDigest: [32]byte{0x66},
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion: 2, ActivePolicyGeneration: 3, ProtectionEpoch: 4,
		OwnershipEpoch: 5, SchemaGeneration: 6, RelationManifestDigest: [32]byte{0x77},
		RoutingVersion: 8, RouteGeneration: 9,
	}
	state := raftservice.ServingState{Identity: identity, Command: command,
		Status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 11, Commit: 20, Applied: 20}}
	config := &rf3CatalogGenesisConfig{
		PlanPath:                 "/does-not-exist/catalog-genesis.plan",
		CatalogPath:              "/does-not-exist/catalog.vibejson",
		InitialNodeDirectoryPath: "/does-not-exist/nodes.vibejson",
		SessionJournal:           "/does-not-exist/gateway/catalog.session",
		ClientID:                 hex.EncodeToString([]byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90}),
		RetryHome:                hex.EncodeToString([]byte{0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98}),
		Distribution:             string(gateway.ReplicatedCatalogDistribution), Shard: string(gateway.ReplicatedCatalogShard),
		ClusterID: hex.EncodeToString(group.ClusterID[:]), ClusterIncarnation: hex.EncodeToString(group.ClusterIncarnation[:]),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, AllocationGeneration: identity.AllocationGeneration,
		ShardIncarnation: hex.EncodeToString(group.ShardIncarnation[:]), GroupID: hex.EncodeToString(group.GroupID[:]),
		MemberID: identity.MemberID, StoreID: hex.EncodeToString(identity.StoreID[:]),
		NodeID: hex.EncodeToString(localNode[:]), NodeIncarnation: 1, Relation: 7,
	}
	return identity, state, config
}

func rf3CatalogGenesisExistingCutFixture(
	t testing.TB, localNode rafttransport.NodeID, drainLifecycle gateway.NodeLifecycle,
) gateway.FrontendDrainRuntimeCut {
	t.Helper()
	local := gateway.NodeRecord{
		NodeID: localNode, Incarnation: 1, ServiceKeyDigest: replication.Digest{0xa1},
		DataEndpoint: "data-local", NativeEndpoint: "native-local", ControlEndpoint: "control-local",
		DataAddress: "127.0.0.1:8101", NativeAddress: "127.0.0.1:8102", ControlAddress: "127.0.0.1:8103",
		FailureDomain: "zone-a", Roles: gateway.NodeRoleStorage | gateway.NodeRoleCatalog,
		Lifecycle: gateway.NodeActive, Revision: 2, CatalogGeneration: 2,
	}
	draining := gateway.NodeRecord{
		NodeID: rafttransport.NodeID{2}, Incarnation: 1, ServiceKeyDigest: replication.Digest{0xa2},
		DataEndpoint: "data-drain", NativeEndpoint: "native-drain", ControlEndpoint: "control-drain",
		GatewayEndpoint: "gateway-drain", DataAddress: "127.0.0.1:8201", NativeAddress: "127.0.0.1:8202",
		ControlAddress: "127.0.0.1:8203", GatewayAddress: "127.0.0.1:8204", FailureDomain: "zone-b",
		Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway, Lifecycle: drainLifecycle, Revision: 3, CatalogGeneration: 2,
		Gateway: gateway.GatewayIdentity{NodeID: rafttransport.NodeID{2}, Incarnation: 4,
			ServiceKeyDigest: replication.Digest{0xa3}, ServiceID: [16]byte{0xa4}, SessionID: [16]byte{0xa5},
			SessionRevision: 2, ParticipantDigest: replication.Digest{0xa6}},
	}
	if drainLifecycle == gateway.NodeDecommissioned {
		draining.RetirementScanDigest = replication.Digest{0xa7}
		draining.RetirementScanDirectoryRevision = 3
		draining.RetirementScanCutRevision = 4
	}
	if !local.Valid() || !draining.Valid() {
		t.Fatalf("invalid existing lifecycle fixture local=%+v draining=%+v", local, draining)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: gateway.ReplicatedCatalogDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Manifests: func() []*distribution.Manifest {
			manifest, err := distribution.NewManifest(gateway.ReplicatedCatalogDistribution, 1, []distribution.Shard{{ID: gateway.ReplicatedCatalogShard, AllocationGeneration: 1,
				Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: []distribution.EndpointID{"data-local", "data-drain", "data-third"}, Epoch: 1}})
			if err != nil {
				t.Fatal(err)
			}
			return []*distribution.Manifest{manifest}
		}(),
	}
	endpoints := map[distribution.EndpointID]string{
		local.DataEndpoint: local.DataAddress, local.NativeEndpoint: local.NativeAddress, local.ControlEndpoint: local.ControlAddress,
		draining.DataEndpoint: draining.DataAddress, draining.NativeEndpoint: draining.NativeAddress, draining.ControlEndpoint: draining.ControlAddress,
		"data-third": "127.0.0.1:8301", "native-third": "127.0.0.1:8302", "control-third": "127.0.0.1:8303",
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 2, nil, nil, []gateway.ReplicatedShardDescriptor{{
		Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard,
		Group: rf3CommandGroup(), AllocationGeneration: 1,
		Command: raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1},
		RangeIdentity: [32]byte{2}, LineageDigest: [32]byte{3}, ForwardingRuleDigest: [32]byte{4},
		RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: [32]byte{5}}},
		Replicas: []gateway.ReplicatedReplicaDescriptor{
			{Member: 1, Node: local.NodeID, NodeIncarnation: local.Incarnation, StoreID: [16]byte{1},
				Endpoint: local.DataEndpoint, NativeEndpoint: local.NativeEndpoint, ControlEndpoint: local.ControlEndpoint},
			{Member: 2, Node: draining.NodeID, NodeIncarnation: draining.Incarnation, StoreID: [16]byte{2},
				Endpoint: draining.DataEndpoint, NativeEndpoint: draining.NativeEndpoint, ControlEndpoint: draining.ControlEndpoint},
			{Member: 3, Node: [16]byte{3}, NodeIncarnation: 1, StoreID: [16]byte{3},
				Endpoint: "data-third", NativeEndpoint: "native-third", ControlEndpoint: "control-third"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	headDigest, err := gateway.ReplicatedCatalogHeadDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return gateway.FrontendDrainRuntimeCut{
		Nodes: gateway.NodeDirectoryCut{Revision: 3, Digest: replication.Digest{0xb1}, CatalogGeneration: 2,
			Nodes: []gateway.NodeRecord{local, draining}},
		Catalog: snapshot, CatalogHeadDigest: headDigest, ServiceDirectoryRevision: 4,
	}
}
