package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type tableRegistrationClient struct {
	catalog           *catalogAuthorityClient
	authority         *ReplicatedCatalogAuthority
	route             ReplicatedRoute
	settlePublication bool
	settled           bool
	witnessReadError  error
	commands          [][]byte
}

func (client *tableRegistrationClient) DoReplicated(
	ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Capability == serviceauthz.CapabilityTopology {
		if client.settled && client.witnessReadError != nil &&
			(request.Operation == shardservice.ReplicatedReadLeader || request.Operation == shardservice.ReplicatedReadFollower) {
			return nil, client.witnessReadError
		}
		if len(request.Command) != 0 {
			client.commands = append(client.commands, bytes.Clone(request.Command))
			// Publish installs its pending witness only after the native call has
			// returned unknown. Release the completion on the outer exact retry.
			if client.settlePublication && client.authority.pendingCatalog != nil {
				client.catalog.holdUnknown = false
			}
		}
		response, err := client.catalog.DoReplicated(ctx, endpoint, request)
		if err == nil && len(request.Command) != 0 && bytes.Equal(request.Command, client.catalog.unknownCommand) {
			client.settled = true
		}
		return response, err
	}
	if request.Capability != serviceauthz.CapabilityDataRead || request.Authority != client.catalog.wantAuthority ||
		request.Fence.Group != client.route.Group {
		return nil, ErrReplicatedUnauthorized
	}
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: client.route.Group, AllocationGeneration: client.route.AllocationGeneration,
			MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation,
			Term: 1, Command: client.route.Command,
		},
		LeaderID: client.route.Replicas[0].Member, Commit: 1, Applied: 1, CheckpointApplied: 1,
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	if request.Operation != shardservice.ReplicatedReadLeader {
		return nil, ErrReplicatedRoute
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedReadMissing, HasState: true, State: state, ReadApplied: 1,
	}, nil
}

func newTableRegistrationFixture(t *testing.T) (*ReplicatedCatalogAuthority, *tableRegistrationClient, *Snapshot) {
	t.Helper()
	authority, catalog, current := newCatalogAuthorityFixture(t)
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	config.Placements[0].Table, profile.Table = "added_table", "added_table"
	descriptor.Group.GroupID[0] ^= 0x80
	descriptor.Group.ShardIncarnation[0] ^= 0x80
	addition, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 1, nil, nil, []ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "added_table", CreateTable: "CREATE TABLE added_table (id TEXT PRIMARY KEY)"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = BuildReplicatedTableAddition(current, addition); err != nil {
		t.Fatal(err)
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := addition.ReplicatedRouteAt(0, replicas[:0])
	if !ok {
		t.Fatal("missing provisioned table route")
	}
	client := &tableRegistrationClient{catalog: catalog, authority: authority, route: route}
	authority.executor.client = client
	return authority, client, addition
}

func TestReplicatedTableRegistrationDoesNotAcknowledgeUnrelatedPendingOperation(t *testing.T) {
	authority, client, addition := newTableRegistrationFixture(t)
	operation := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0x61}, Kind: ReplicatedOperationSplit, State: ReplicatedOperationPlanned,
		Revision: 1, CatalogGeneration: authority.holder.Current().Generation(),
		Cursor: [8]uint64{1}, Proof: [32]byte{7},
	})
	client.catalog.unknownNext = true
	if err := authority.SubmitOperation(context.Background(), operation); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unrelated operation must be pending: %v", err)
	}
	pending := authority.session.PendingCommand()
	client.catalog.holdUnknown = false
	client.commands = nil
	if err := authority.RegisterProvisionedTable(context.Background(), addition); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("unpublished table registration must conflict, got %v", err)
	}
	if authority.session.Status().Pending || len(client.commands) == 0 {
		t.Fatal("unrelated pending command was not settled")
	}
	for _, command := range client.commands {
		if !bytes.Equal(command, pending) {
			t.Fatal("registration submitted a fresh command instead of only settling the pending bytes")
		}
	}
	current, err := authority.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := current.Placement("added_table"); found {
		t.Fatal("registration unexpectedly published a new table")
	}
	loaded, err := authority.ReadOperation(context.Background(), operation.ID)
	if err != nil || !loaded.Equal(operation) {
		t.Fatalf("unrelated operation changed: %v", err)
	}
}

func TestReplicatedTableRegistrationVerifiesOwnExactPendingPublication(t *testing.T) {
	authority, client, addition := newTableRegistrationFixture(t)
	client.catalog.unknownNext = true
	client.settlePublication = true
	if err := authority.RegisterProvisionedTable(context.Background(), addition); err != nil {
		t.Fatalf("exact pending publication did not settle: %v", err)
	}
	if authority.session.Status().Pending || !client.settled || len(client.commands) < 2 {
		t.Fatal("fixture did not exercise the outer pending retry")
	}
	for _, command := range client.commands {
		if !bytes.Equal(command, client.catalog.unknownCommand) {
			t.Fatal("publication retry changed native command bytes")
		}
	}
	current, err := authority.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next, err := BuildReplicatedTableAddition(current, addition); err != nil || next != current {
		t.Fatalf("successful registration lacks its certified table witness: %v", err)
	}
	client.commands = nil
	if err = authority.RegisterProvisionedTable(context.Background(), addition); err != nil || len(client.commands) != 0 {
		t.Fatalf("existing table registration must be read-only: %v", err)
	}
}

func TestReplicatedTableRegistrationPreservesUnknownPublication(t *testing.T) {
	authority, client, addition := newTableRegistrationFixture(t)
	client.catalog.unknownNext = true
	err := authority.RegisterProvisionedTable(context.Background(), addition)
	if !errors.Is(err, ErrReplicatedCatalogPending) || IsReplicatedReadRetryable(err) {
		t.Fatalf("unresolved publication lost its terminal unknown classification: %v", err)
	}
	if !authority.session.Status().Pending || !bytes.Equal(authority.session.PendingCommand(), client.catalog.unknownCommand) {
		t.Fatal("unresolved publication lost its exact pending command")
	}
	if len(client.commands) == 0 || len(client.commands) > authority.executor.maxAttempts*4 {
		t.Fatalf("publication exceeded its existing retry bound: %d commands", len(client.commands))
	}
	for _, command := range client.commands {
		if !bytes.Equal(command, client.catalog.unknownCommand) {
			t.Fatal("unknown publication was replaced with a new command")
		}
	}
}

func TestReplicatedTableRegistrationWitnessReadFailureDoesNotPermitWriteReplay(t *testing.T) {
	for _, refusal := range []struct {
		name string
		err  error
	}{
		{"leader", ErrReplicatedLeader},
		{"unauthorized", ErrReplicatedUnauthorized},
		{"cancelled", context.Canceled},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			authority, client, addition := newTableRegistrationFixture(t)
			client.catalog.unknownNext = true
			client.settlePublication = true
			client.witnessReadError = refusal.err
			err := authority.RegisterProvisionedTable(context.Background(), addition)
			if !errors.Is(err, ErrReplicatedCatalogPending) || !errors.Is(err, refusal.err) || IsReplicatedReadRetryable(err) {
				t.Fatalf("unverified publication allowed a fresh retry: %v", err)
			}
			if authority.session.Status().Pending || !client.settled {
				t.Fatal("fixture failed before the post-settlement witness read")
			}
			for _, command := range client.commands {
				if !bytes.Equal(command, client.catalog.unknownCommand) {
					t.Fatal("witness read failure submitted a new publication")
				}
			}
		})
	}
}

func TestReplicatedProvisionProfileAcceptsSchemaSuccessor(t *testing.T) {
	origin := ReplicatedTableProfile{Table: "employees", Relation: 1, PrimaryKey: "/id",
		SchemaGeneration: 1, LogicalSchemaDigest: [32]byte{1}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20}
	current := origin
	current.SchemaGeneration = 2
	current.LogicalSchemaDigest = [32]byte{2}
	if !replicatedProvisionProfileMatches(current, origin) {
		t.Fatal("authorized schema successor rejected as conflicting provisioning")
	}
	for _, mutate := range []func(*ReplicatedTableProfile){
		func(p *ReplicatedTableProfile) { p.Relation++ },
		func(p *ReplicatedTableProfile) { p.PrimaryKey = "/other" },
		func(p *ReplicatedTableProfile) { p.MaxDocumentBytes++ },
		func(p *ReplicatedTableProfile) { p.SchemaGeneration = 0 },
	} {
		changed := current
		mutate(&changed)
		if replicatedProvisionProfileMatches(changed, origin) {
			t.Fatal("incompatible provision profile accepted")
		}
	}
	sameGeneration := origin
	sameGeneration.LogicalSchemaDigest[0] ^= 1
	if replicatedProvisionProfileMatches(sameGeneration, origin) {
		t.Fatal("same-generation logical schema conflict accepted")
	}
}

func TestReplicatedTableAdditionResumeAcceptsEvolvedDeclaration(t *testing.T) {
	origin := []ReplicatedTableDeclaration{{Table: "messages", CreateTable: "CREATE TABLE messages (PRIMARY KEY (id))"}}
	evolved := []ReplicatedTableDeclaration{{Table: "messages", CreateTable: "CREATE TABLE messages (id TEXT PRIMARY KEY, city TEXT)"}}
	if !replicatedProvisionDeclarationsMatch(2, 1, evolved, origin) {
		t.Fatal("evolved declaration rejected after authenticated schema successor")
	}
	if replicatedProvisionDeclarationsMatch(1, 1, evolved, origin) {
		t.Fatal("same-generation declaration conflict accepted")
	}
}
