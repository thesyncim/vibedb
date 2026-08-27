package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedExecutionPinDelegationKeepsOriginalAuthorityAndCurrentPolicy(t *testing.T) {
	peer, original, nondelegate := authorizationNode(21), authorizationNode(12), authorizationNode(22)
	gate := authorizationGate(t, 1,
		serviceauthz.Entry{Node: peer, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: original, Capabilities: serviceauthz.CapabilityExecutionPin},
		serviceauthz.Entry{Node: nondelegate, Capabilities: serviceauthz.CapabilityExecutionPin},
	)
	server := &ReplicatedServer{authorization: gate}
	fence := testReplicatedFence()
	request := &ReplicatedRequest{Operation: ReplicatedPropose, Fence: fence,
		Authority:  serviceauthz.Authority{Node: original, Generation: 1},
		Capability: serviceauthz.CapabilityExecutionPin, Command: testReplicatedExecutionPinCommand(t, fence)}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		t.Fatal(err)
	}
	if !server.authorizeReplicated(peer, request) || !replicatedExecutionPinAuthorityMatches(request, command) {
		t.Fatal("existing delegate path rejected exact authorized original principal")
	}
	if server.authorizeReplicated(nondelegate, request) {
		t.Fatal("execution-pin authority without Delegate forwarded another principal")
	}
	request.Authority.Node = peer
	if replicatedExecutionPinAuthorityMatches(request, command) {
		t.Fatal("current gateway identity substituted into retained command authority")
	}
	request.Authority.Node = original
	server.authorization = authorizationGate(t, 1,
		serviceauthz.Entry{Node: peer, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: original, Capabilities: serviceauthz.CapabilityDataRead})
	if server.authorizeReplicated(peer, request) {
		t.Fatal("revoked original execution-pin capability remained usable")
	}
	server.authorization = authorizationGate(t, 2,
		serviceauthz.Entry{Node: peer, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: original, Capabilities: serviceauthz.CapabilityExecutionPin})
	if server.authorizeReplicated(peer, request) {
		t.Fatal("retained command bypassed revoked policy generation")
	}
	request.Authority.Generation = 2
	if replicatedExecutionPinAuthorityMatches(request, command) {
		t.Fatal("retained command authority generation was rewritten")
	}
}
