package gatewayruntime

import (
	"context"
	"errors"
	"net"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
)

func startGatewayPostgreSQL(ctx context.Context, address string, executor *gateway.Executor, authority serviceauthz.Authority, write func(context.Context, serviceauthz.Authority, gateway.Query) (*gateway.Result, error), logf func(string, ...any), ddl ...func(context.Context, serviceauthz.Authority, string) error) (*pgwire.Server, error) {
	return startGatewayPostgreSQLWithFrontend(ctx, address, executor, authority, write, logf, nil, ddl...)
}

// startGatewayPostgreSQLWithFrontend keeps the public PostgreSQL listener
// behind the same admission fence as the native frontend. The wrapper owns
// only token admission; pgwire remains the authority for exact connection and
// SQL-session counts.
func startGatewayPostgreSQLWithFrontend(ctx context.Context, address string, executor *gateway.Executor, authority serviceauthz.Authority, write func(context.Context, serviceauthz.Authority, gateway.Query) (*gateway.Result, error), logf func(string, ...any), frontend *frontendAdmission, ddl ...func(context.Context, serviceauthz.Authority, string) error) (*pgwire.Server, error) {
	if err := requireLoopbackListen(address); err != nil {
		return nil, err
	}
	backend := &gateway.PostgreSQLBackend{
		Executor: executor, Write: write, Authorize: func(identity pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			if identity.User != "local" || identity.Database != "vibedb" {
				return serviceauthz.Authority{}, gateway.ErrReplicatedUnauthorized
			}
			return authority, nil
		},
	}
	if len(ddl) == 1 {
		backend.DDL = ddl[0]
	}
	server, err := pgwire.NewServerWithBackend(backend, pgwire.Options{Auth: pgwire.Trust(), Database: "vibedb", MaxConnections: 16, MaxResultRows: 100000, MaxResultBytes: shardservice.MaxReplicatedSQLResultBytes})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		_ = server.Close()
		return nil, err
	}
	if frontend != nil {
		listener = &frontendPGAdmissionListener{Listener: listener, frontend: frontend}
	}
	go func() { <-ctx.Done(); _ = server.Close() }()
	go func() {
		if err := server.Serve(listener); err != nil && ctx.Err() == nil &&
			!errors.Is(err, pgwire.ErrServerDraining) {
			logf("gateway: PostgreSQL stopped: %v", err)
		}
	}()
	logf("gateway: local PostgreSQL RF3 endpoint %s (user local, database vibedb; durable auto-commit writes)", listener.Addr())
	return server, nil
}
