package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	gatewayDDLForwardTimeout = 2 * time.Minute
	// SQL can expand sixfold when JSON escaped. Bound the complete envelope
	// independently, before decoding, as well as the decoded schema statement.
	maxGatewayDDLForwardRequest  = 6*sqldriver.ReplicatedChildSchemaMaxBytes + 1024
	maxGatewayDDLForwardResponse = 32 << 10
)

type gatewayDDLForwardContextKey struct{}

type gatewayDDLForwardRequest struct {
	Op         string `json:"op"`
	Actor      string `json:"actor"`
	Generation uint64 `json:"generation"`
	Deadline   int64  `json:"deadline_unix_nano"`
	SQL        string `json:"sql"`
}

// The closed completion class controls retry safety. An uncertain write or
// reply is always unknown, even when cancellation is also present. There is no
// automatic reconnect/replay and no invented per-frontend schema gate ID.
type gatewayDDLForwardResponse struct {
	Op      string `json:"op"`
	Status  string `json:"status"`
	State   string `json:"sqlstate"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

type gatewayDDLForwardError struct{ response gatewayDDLForwardResponse }

func (e *gatewayDDLForwardError) Error() string    { return e.response.Message }
func (e *gatewayDDLForwardError) SQLState() string { return e.response.State }
func (e *gatewayDDLForwardError) SQLHint() string  { return e.response.Hint }
func (e *gatewayDDLForwardError) Is(target error) bool {
	return (target == durable.ErrCommitOutcomeUnknown && e.response.Status == "unknown") ||
		(target == gateway.ErrReplicatedUnauthorized && e.response.State == "42501")
}

type gatewayDDLForwardOwner struct {
	capability *gateway.ClientTLS
	roster     map[rafttransport.NodeID]struct{}
	run        func(context.Context, serviceauthz.Authority, string) error
	admission  chan struct{}
}

func (runtime *Runtime) openDDL() error {
	config := runtime.config
	if config.ControlParticipantOnly {
		if config.PGListenAddress == "" {
			return nil
		}
		if runtime.clientTLS == nil || runtime.replicaControlManifest == nil ||
			config.DDLOwnerNode == config.InternalAuthority.Node {
			return fmt.Errorf("%w: participant DDL requires a distinct authenticated owner", ErrInvalidConfig)
		}
		found := false
		for _, endpoint := range runtime.replicaControlManifest.Gateways {
			found = found || endpoint.Member.Node == config.DDLOwnerNode
		}
		if !found || config.Authorization.Check(config.DDLOwnerNode,
			serviceauthz.CapabilitySchema|serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
			return fmt.Errorf("%w: DDL owner must have schema authority and belong to the complete gateway roster", ErrInvalidConfig)
		}
		client, err := servicetls.NewClient(servicetls.ClientOptions{
			TLS: config.TLSProfile, Class: rafttransport.TrafficGatewayClient,
			Endpoints: []servicetls.Endpoint{{Address: config.DDLOwnerAddress, Node: config.DDLOwnerNode}},
			Dial: func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}, HandshakeDeadline: servicetls.FixedDeadline(config.TLSHandshakeTimeout),
			MaxConnections: 16, MaxHandshakes: 16,
		})
		if err != nil {
			return fmt.Errorf("open authenticated DDL forwarding: %w", err)
		}
		runtime.ddlForwardTLS = client
		runtime.pgDDL = func(ctx context.Context, actor serviceauthz.Authority, text string) error {
			return forwardGatewayDDL(ctx, runtime.clientTLS, client.Dial, config.DDLOwnerAddress, actor, text)
		}
		return nil
	}
	if config.PGDDLSocket == "" {
		return nil
	}
	run := newGatewayDevDDL(config.PGDDLSocket, runtime.authority, runtime.schemaDDL)
	// Check the original actor at the common local/forwarded DDL boundary.
	// Schema recovery itself keeps its persisted owner principal unchanged.
	runtime.pgDDL = func(ctx context.Context, actor serviceauthz.Authority, text string) error {
		actorCtx, err := serviceauthz.WithAuthority(ctx, actor)
		if err != nil {
			return err
		}
		if runtime.clientTLS != nil && runtime.clientTLS.Authorize(actorCtx, serviceauthz.CapabilitySchema, nil) != serviceauthz.DecisionAllow {
			return gateway.ErrReplicatedUnauthorized
		}
		return run(actorCtx, actor, text)
	}
	if runtime.clientTLS != nil && runtime.replicaControlManifest != nil {
		roster := make(map[rafttransport.NodeID]struct{}, len(runtime.replicaControlManifest.Gateways))
		for _, endpoint := range runtime.replicaControlManifest.Gateways {
			roster[endpoint.Member.Node] = struct{}{}
		}
		runtime.ddlForwardOwner = &gatewayDDLForwardOwner{capability: runtime.clientTLS,
			roster: roster, run: runtime.pgDDL, admission: make(chan struct{}, 16)}
	}
	return nil
}

func validateGatewayForwardDDL(text string) error {
	if len(text) == 0 || len(text) > sqldriver.ReplicatedChildSchemaMaxBytes {
		return sqlast.NewFeatureNotSupportedError(text, 0, "distributed DDL exceeds its statement bound")
	}
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		return err
	}
	if tree.CreateIndex != nil && tree.CreateIndex.Unique {
		return sqlast.NewFeatureNotSupportedError(text, 0, "distributed CREATE UNIQUE INDEX requires a coordinated uniqueness build and is not supported")
	}
	switch tree.Kind {
	case sqlast.KindCreateTable, sqlast.KindDropTable, sqlast.KindCreateIndex,
		sqlast.KindAlterTable, sqlast.KindDropIndex, sqlast.KindTruncate:
		if tree.Params() == 0 {
			return nil
		}
	}
	return sqlast.NewFeatureNotSupportedError(text, 0, "forwarded schema commands require one bounded DDL statement without parameters")
}

func (owner *gatewayDDLForwardOwner) execute(ctx context.Context, raw []byte) gatewayDDLForwardResponse {
	if owner == nil || owner.capability == nil || owner.run == nil || owner.admission == nil {
		return gatewayDDLForwardFailure("error", "0A000", "coordinated DDL is unavailable on this gateway")
	}
	peer, authenticated := serviceauthz.FromContext(ctx)
	_, member := owner.roster[peer.Node]
	if !authenticated || !member || owner.capability.Authorize(ctx,
		serviceauthz.CapabilityDelegate|serviceauthz.CapabilitySchema, nil) != serviceauthz.DecisionAllow {
		return gatewayDDLForwardFailure("error", "42501", "DDL forwarding authorization denied")
	}
	var request gatewayDDLForwardRequest
	if len(raw) == 0 || len(raw) > maxGatewayDDLForwardRequest || vibejson.Unmarshal(raw, &request) != nil {
		return gatewayDDLForwardFailure("error", "08P01", "invalid DDL forwarding envelope")
	}
	canonical, err := vibejson.Marshal(&request)
	var node rafttransport.NodeID
	if err != nil || !bytes.Equal(raw, canonical) || request.Op != "ddl_forward" ||
		len(request.Actor) != hex.EncodedLen(len(node)) {
		return gatewayDDLForwardFailure("error", "08P01", "noncanonical DDL forwarding envelope")
	}
	if _, err = hex.Decode(node[:], []byte(request.Actor)); err != nil || hex.EncodeToString(node[:]) != request.Actor {
		return gatewayDDLForwardFailure("error", "08P01", "invalid forwarded DDL actor")
	}
	actor := serviceauthz.Authority{Node: node, Generation: request.Generation}
	actorCtx, err := serviceauthz.WithAuthority(ctx, actor)
	if err != nil || owner.capability.Authorize(actorCtx, serviceauthz.CapabilitySchema, nil) != serviceauthz.DecisionAllow {
		return gatewayDDLForwardFailure("error", "42501", "forwarded DDL actor authorization denied")
	}
	if request.Deadline <= time.Now().UnixNano() {
		return gatewayDDLForwardFailure("error", "57014", "forwarded DDL deadline expired before execution")
	}
	deadline := min(request.Deadline, time.Now().Add(gatewayDDLForwardTimeout).UnixNano())
	actorCtx, cancel := context.WithDeadline(actorCtx, time.Unix(0, deadline))
	defer cancel()
	if err := validateGatewayForwardDDL(request.SQL); err != nil {
		return gatewayDDLForwardResult(err)
	}
	select {
	case owner.admission <- struct{}{}:
		defer func() { <-owner.admission }()
	default:
		return gatewayDDLForwardFailure("error", "53300", "DDL owner admission bound reached")
	}
	if actorCtx.Err() != nil {
		return gatewayDDLForwardFailure("error", "57014", "forwarded DDL canceled before execution")
	}
	return gatewayDDLForwardResult(owner.run(actorCtx, actor, request.SQL))
}

func serveGatewayDDLForward(ctx context.Context, connection net.Conn, owner *gatewayDDLForwardOwner, raw []byte) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The stream carries exactly one command. A peer disconnect (including PG
	// cancellation on the forwarding frontend) cancels the owner operation;
	// reconnecting would have unknown completion and is never attempted here.
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		var extra [1]byte
		_, _ = connection.Read(extra[:])
		cancel()
	}()
	response := owner.execute(ctx, raw)
	_ = writeGatewayDDLForwardResponse(connection, response)
	_ = connection.Close()
	<-peerDone
}

func forwardGatewayDDL(ctx context.Context, capability *gateway.ClientTLS, dial gateway.DialFunc,
	address string, actor serviceauthz.Authority, text string,
) error {
	actorCtx, err := serviceauthz.WithAuthority(ctx, actor)
	if err != nil || capability == nil || capability.Authorize(actorCtx, serviceauthz.CapabilitySchema, nil) != serviceauthz.DecisionAllow {
		return gateway.ErrReplicatedUnauthorized
	}
	if err := validateGatewayForwardDDL(text); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(actorCtx, gatewayDDLForwardTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	request := gatewayDDLForwardRequest{Op: "ddl_forward", Actor: hex.EncodeToString(actor.Node[:]),
		Generation: actor.Generation, Deadline: deadline.UnixNano(), SQL: text}
	raw, err := vibejson.Marshal(&request)
	if err != nil || len(raw) > maxGatewayDDLForwardRequest {
		return errors.Join(err, ErrInvalidConfig)
	}
	connection, err := dial(ctx, address)
	if err != nil {
		return err
	} // No command bytes have been sent.
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := connection.SetDeadline(deadline); err != nil {
		return err
	}
	// Any short/failed write may have delivered a complete command. Never
	// replace unknown completion with a cancellation or retryable network error.
	raw = append(raw, '\n')
	if n, err := connection.Write(raw); err != nil || n != len(raw) {
		if n != len(raw) {
			err = errors.Join(err, io.ErrShortWrite)
		}
		return errors.Join(durable.ErrCommitOutcomeUnknown, err, ctx.Err())
	}
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 4096), maxGatewayDDLForwardResponse)
	if !scanner.Scan() {
		return errors.Join(durable.ErrCommitOutcomeUnknown, scanner.Err(), io.ErrUnexpectedEOF, ctx.Err())
	}
	var response gatewayDDLForwardResponse
	if vibejson.Unmarshal(scanner.Bytes(), &response) != nil {
		return durable.ErrCommitOutcomeUnknown
	}
	canonical, err := vibejson.Marshal(&response)
	if err != nil || !bytes.Equal(canonical, scanner.Bytes()) || response.Op != "ddl_result" ||
		len(response.Message) > 4096 || len(response.Hint) > 1024 {
		return durable.ErrCommitOutcomeUnknown
	}
	if response.Status == "ok" && response.State == "" && response.Message == "" && response.Hint == "" {
		return nil
	}
	if (response.Status != "error" && response.Status != "unknown") || !validGatewayDDLSQLState(response.State) ||
		response.Message == "" || (response.Status == "unknown") != (response.State == "40003") {
		return durable.ErrCommitOutcomeUnknown
	}
	return &gatewayDDLForwardError{response: response}
}

func writeGatewayDDLForwardResponse(connection net.Conn, response gatewayDDLForwardResponse) error {
	raw, err := vibejson.Marshal(&response)
	if err != nil {
		return err
	}
	if err := connection.SetWriteDeadline(time.Now().Add(defaultNativeResponseWriteTimeout)); err != nil {
		return err
	}
	raw = append(raw, '\n')
	n, err := connection.Write(raw)
	if err == nil && n != len(raw) {
		return io.ErrShortWrite
	}
	return err
}

func gatewayDDLForwardFailure(status, state, message string) gatewayDDLForwardResponse {
	return gatewayDDLForwardResponse{Op: "ddl_result", Status: status, State: state, Message: message}
}

func gatewayDDLForwardResult(err error) gatewayDDLForwardResponse {
	if err == nil {
		return gatewayDDLForwardResponse{Op: "ddl_result", Status: "ok"}
	}
	response := gatewayDDLForwardFailure("error", "", err.Error())
	if len(response.Message) > 4096 {
		response.Message = response.Message[:4096]
	}
	var unsupported *sqlast.FeatureNotSupportedError
	var parse *sqlast.ParseError
	var diagnostic interface{ SQLState() string }
	switch {
	case errors.Is(err, durable.ErrCommitOutcomeUnknown):
		response.Status, response.State = "unknown", "40003"
	case errors.As(err, &diagnostic) && validGatewayDDLSQLState(diagnostic.SQLState()):
		response.State = diagnostic.SQLState()
		if response.State == "40003" {
			response.Status = "unknown"
		}
	case errors.Is(err, gateway.ErrReplicatedUnauthorized):
		response.State = "42501"
	case errors.Is(err, sqldriver.ErrTableExists):
		response.State = "42P07"
	case errors.Is(err, sqldriver.ErrTableNotFound):
		response.State = "42P01"
	case errors.Is(err, sqldriver.ErrColumnExists):
		response.State = "42701"
	case errors.Is(err, sqldriver.ErrIndexExists):
		response.State = "42710"
	case errors.Is(err, sqldriver.ErrIndexNotFound):
		response.State = "42704"
	case errors.Is(err, sqldriver.ErrDependentObjects):
		response.State = "2BP01"
	case errors.As(err, &unsupported):
		response.State = "0A000"
	case errors.As(err, &parse):
		response.State = "42601"
	default:
		// Unclassified runtime failures can occur after a durable schema step;
		// the single owner's existing journal/recovery settles those steps.
		response.Status, response.State = "unknown", "40003"
	}
	var hinted interface{ SQLHint() string }
	if errors.As(err, &hinted) {
		response.Hint = hinted.SQLHint()
	}
	if len(response.Hint) > 1024 {
		response.Hint = response.Hint[:1024]
	}
	return response
}

func validGatewayDDLSQLState(state string) bool {
	if len(state) != 5 || state[:2] == "00" {
		return false
	}
	for _, value := range state {
		if (value < '0' || value > '9') && (value < 'A' || value > 'Z') {
			return false
		}
	}
	return true
}
