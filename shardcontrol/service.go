package shardcontrol

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrUnauthorized   = errors.New("shardcontrol: action is not authorized")
	ErrOutcomeUnknown = errors.New("shardcontrol: control outcome is unknown")
)

const AbsoluteMaxGrants = 65536

// ActionGrant is an exact certificate Node principal and closed action mask.
// Bit zero grants action one. No subject strings enter authorization.
type ActionGrant struct {
	Node    rafttransport.NodeID
	Actions uint16
}

// Authorizer is an immutable sorted capability table. It complements the TLS
// trust-domain and node allowlist with per-action authorization.
type Authorizer struct{ grants []ActionGrant }

func NewAuthorizer(grants []ActionGrant) (*Authorizer, error) {
	if len(grants) == 0 || len(grants) > AbsoluteMaxGrants {
		return nil, ErrUnauthorized
	}
	owned := slices.Clone(grants)
	slices.SortFunc(owned, func(left, right ActionGrant) int { return bytes.Compare(left.Node[:], right.Node[:]) })
	for index := range owned {
		if owned[index].Node == (rafttransport.NodeID{}) || owned[index].Actions == 0 ||
			owned[index].Actions&^uint16((1<<uint(ActionReconcileSplit))-1) != 0 ||
			index != 0 && owned[index-1].Node == owned[index].Node {
			return nil, ErrUnauthorized
		}
	}
	return &Authorizer{grants: owned}, nil
}

func (authorizer *Authorizer) allows(node rafttransport.NodeID, action Action) bool {
	if authorizer == nil || !validAction(action) {
		return false
	}
	index, found := slices.BinarySearchFunc(authorizer.grants, node, func(grant ActionGrant, node rafttransport.NodeID) int {
		return bytes.Compare(grant.Node[:], node[:])
	})
	return found && authorizer.grants[index].Actions&(1<<uint(action-1)) != 0
}

// Executor must durably settle (Operation, Step) before returning Accepted.
// An exact replay must return the byte-identical ResultDigest and Payload.
type Executor interface {
	ExecuteControl(context.Context, rafttransport.PeerIdentity, Request) (Response, error)
}

// Server owns one authenticated control request per TLS stream.
type Server struct {
	authorizer *Authorizer
	executor   Executor
}

func NewServer(authorizer *Authorizer, executor Executor) (*Server, error) {
	if authorizer == nil || executor == nil {
		return nil, ErrUnauthorized
	}
	return &Server{authorizer: authorizer, executor: executor}, nil
}

func (server *Server) ServeConnection(ctx context.Context, connection rafttransport.PeerConnection) error {
	if server == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrUnauthorized
	}
	request, err := ReadRequest(connection)
	if err != nil {
		return err
	}
	identity := connection.PeerIdentity()
	if !server.authorizer.allows(identity.Node, request.Action) {
		return ErrUnauthorized
	}
	response, err := server.executor.ExecuteControl(ctx, identity, request)
	if err != nil {
		return err
	}
	if response.Operation != request.Operation || response.Step != request.Step || !validResponse(&response) {
		return ErrWire
	}
	return WriteResponse(connection, &response)
}

// Dial opens one authenticated control stream. The caller supplies the
// rotation-safe TLS dial capability and a hard protocol deadline.
type Dial func(context.Context, string) (net.Conn, error)

type Client struct {
	dial    Dial
	address string
	timeout time.Duration
}

func NewClient(dial Dial, address string, timeout time.Duration) (*Client, error) {
	if dial == nil || address == "" || timeout <= 0 {
		return nil, ErrWire
	}
	return &Client{dial: dial, address: address, timeout: timeout}, nil
}

func (client *Client) Execute(ctx context.Context, request Request) (Response, error) {
	if client == nil || ctx == nil || !validRequest(&request) {
		return Response{}, ErrWire
	}
	connection, err := client.dial(ctx, client.address)
	if err != nil {
		return Response{}, err
	}
	defer connection.Close()
	deadline := time.Now().Add(client.timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err = connection.SetDeadline(deadline); err != nil {
		return Response{}, err
	}
	if err = WriteRequest(connection, &request); err != nil {
		return Response{}, errors.Join(ErrOutcomeUnknown, err)
	}
	response, err := ReadResponse(connection)
	if err != nil {
		return Response{}, errors.Join(ErrOutcomeUnknown, err)
	}
	if response.Operation != request.Operation || response.Step != request.Step {
		return Response{}, ErrWire
	}
	return response, nil
}
