package replication

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/routegate"
)

func TestMembershipStableAuthorityHasDistinctBoundedWireIdentity(t *testing.T) {
	for _, routeSession := range []bool{false, true} {
		command := testCommand()
		stable := CommandAuthorityMembershipStableData
		if routeSession {
			command.AuthorityClass = CommandAuthorityRouteSession
			stable = CommandAuthorityMembershipStableRouteSession
			command.Kind, command.Batches = CommandRouteGate, nil
			var err error
			command.RouteGate, err = routegate.AppendCommand(nil, routegate.Command{
				Operation: routegate.OperationAcquireShared, Epoch: 1,
				Identity: routegate.Identity(sha256.Sum256([]byte("command"))),
				Binding:  routegate.Binding(sha256.Sum256([]byte("binding"))),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		legacy := encodeCommand(t, command)
		command.AuthorityClass = stable
		encoded := encodeCommand(t, command)
		if len(encoded) != len(legacy) || bytes.Equal(encoded, legacy) {
			t.Fatal("stable authority must be a distinct identity without larger commands")
		}
		view, err := OpenCommand(encoded)
		if err != nil || view.AuthorityClass != stable {
			t.Fatalf("stable authority round trip: %v", err)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, err := OpenCommand(encoded); err != nil {
				panic(err)
			}
		}); allocations != 0 {
			t.Fatalf("command decode allocations=%g", allocations)
		}
		if routeSession {
			command.Kind, command.RouteGate = CommandMutationBatch, nil
			command.Batches = testCommand().Batches
			if _, err := AppendCommand(nil, command); err == nil {
				t.Fatal("route-session authority can mutate data")
			}
		}
	}
}

func TestCommandMembershipMatchesRequiresExplicitStableAuthority(t *testing.T) {
	for _, class := range []CommandAuthorityClass{
		CommandAuthorityData, CommandAuthorityTopology, CommandAuthorityRequestLedger,
		CommandAuthorityExecutionPin, CommandAuthorityRouteSession, CommandAuthorityExecutionSession,
		CommandAuthorityMembershipStableData, CommandAuthorityMembershipStableRouteSession,
	} {
		if !CommandMembershipMatches(class, 3, 3) {
			t.Fatalf("class %d rejected exact membership", class)
		}
		if CommandMembershipMatches(class, 2, 3) != IsMembershipStableAuthority(class) {
			t.Fatalf("class %d changed legacy semantics", class)
		}
		if CommandMembershipMatches(class, 4, 3) || CommandMembershipMatches(class, 0, 3) || CommandMembershipMatches(class, 0, 0) {
			t.Fatalf("class %d accepted future/invalid membership", class)
		}
	}
}
