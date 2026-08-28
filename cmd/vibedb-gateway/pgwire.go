package main

import (
	"context"
	"net"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
)

func startGatewayPostgreSQL(ctx context.Context, address string, executor *gateway.Executor, authority serviceauthz.Authority, logf func(string, ...any)) (*pgwire.Server, error) {
	if err := requireLoopbackListen(address); err != nil {
		return nil, err
	}
	server, err := pgwire.NewServerWithBackend(&gateway.PostgreSQLBackend{
		Executor: executor, Authorize: func(identity pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			if identity.User != "local" || identity.Database != "vibedb" {
				return serviceauthz.Authority{}, gateway.ErrReplicatedUnauthorized
			}
			return authority, nil
		},
	}, pgwire.Options{Auth: pgwire.Trust(), Database: "vibedb", MaxConnections: 16, MaxResultRows: 100000, MaxResultBytes: shardservice.MaxReplicatedSQLResultBytes})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		_ = server.Close()
		return nil, err
	}
	go func() { <-ctx.Done(); _ = server.Close() }()
	go func() {
		if err := server.Serve(listener); err != nil && ctx.Err() == nil {
			logf("gateway: PostgreSQL stopped: %v", err)
		}
	}()
	logf("gateway: local PostgreSQL RF3 read endpoint %s (user local, database vibedb; no SQL writes)", listener.Addr())
	return server, nil
}
