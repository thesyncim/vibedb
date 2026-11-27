package gatewayruntime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
)

// AllowedPGCapabilities is the closed set of SQL-facing capabilities a
// PostgreSQL SCRAM user may be granted. Control-plane capabilities (membership,
// topology, backup, restore) are never granted through pgwire.
const AllowedPGCapabilities = serviceauthz.CapabilityDataRead |
	serviceauthz.CapabilityDataWrite |
	serviceauthz.CapabilitySchema

const (
	defaultPGDatabase       = "vibedb"
	defaultPGMaxConnections = 16
	maxPGMaxConnections     = 4096
)

// PGUserCredential binds one PostgreSQL SCRAM-SHA-256 user to a verifier and
// an explicit least-privilege SQL capability subset.
type PGUserCredential struct {
	User         string
	Verifier     pgwire.Verifier
	Capabilities serviceauthz.Capability
}

// PGEndpointOptions configures the gateway's PostgreSQL listener.
//
// Security contract:
//   - When Users is empty (dev loopback mode), the listener is strictly
//     confined to loopback (127.0.0.1 / ::1), uses Trust auth for user "local",
//     and grants the gateway's internal SQL authority.
//   - When Users is non-empty, SCRAM-SHA-256 authentication and per-user
//     capability enforcement are active.
//   - Binding a non-loopback address REQUIRES both TLSConfig != nil (with
//     RequireTLS unconditionally enforced) AND len(Users) > 0; plaintext or
//     Trust auth on a non-loopback address fails closed.
type PGEndpointOptions struct {
	Address        string
	TLSConfig      *tls.Config
	Users          []PGUserCredential
	Database       string
	MaxConnections int
}

func validatePGEndpointOptions(opts PGEndpointOptions) error {
	if opts.Address == "" {
		return nil
	}
	loopbackErr := requireLoopbackListen(opts.Address)
	if loopbackErr != nil {
		if opts.TLSConfig == nil {
			return fmt.Errorf("non-loopback PostgreSQL listener requires TLSConfig")
		}
		if len(opts.Users) == 0 {
			return fmt.Errorf("non-loopback PostgreSQL listener requires SCRAM Users")
		}
	}
	if opts.MaxConnections < 0 || opts.MaxConnections > maxPGMaxConnections {
		return fmt.Errorf("PostgreSQL MaxConnections %d out of range [0,%d]", opts.MaxConnections, maxPGMaxConnections)
	}
	seen := make(map[string]struct{}, len(opts.Users))
	for i, u := range opts.Users {
		if u.User == "" {
			return fmt.Errorf("PostgreSQL user %d has empty username", i)
		}
		if _, dup := seen[u.User]; dup {
			return fmt.Errorf("duplicate PostgreSQL user %q", u.User)
		}
		seen[u.User] = struct{}{}
		if !u.Verifier.Valid() {
			return fmt.Errorf("PostgreSQL user %q has invalid SCRAM verifier", u.User)
		}
		if u.Capabilities == 0 || u.Capabilities&^AllowedPGCapabilities != 0 {
			return fmt.Errorf("PostgreSQL user %q capabilities 0x%x outside allowed SQL set", u.User, uint64(u.Capabilities))
		}
	}
	return nil
}

// startGatewayPostgreSQLWithFrontend keeps the public PostgreSQL listener
// behind the same admission fence as the native frontend. The wrapper owns
// only token admission; pgwire remains the authority for exact connection and
// SQL-session counts.
func startGatewayPostgreSQLWithFrontend(
	ctx context.Context,
	address string,
	executor *gateway.Executor,
	authority serviceauthz.Authority,
	write func(context.Context, serviceauthz.Authority, gateway.Query) (*gateway.Result, error),
	logf func(string, ...any),
	frontend *frontendAdmission,
	ddl ...func(context.Context, serviceauthz.Authority, string) error,
) (*pgwire.Server, error) {
	return startGatewayPostgreSQLWithOptions(
		ctx,
		PGEndpointOptions{Address: address},
		executor, authority, write, logf, frontend, ddl...,
	)
}

func startGatewayPostgreSQLWithOptions(
	ctx context.Context,
	opts PGEndpointOptions,
	executor *gateway.Executor,
	authority serviceauthz.Authority,
	write func(context.Context, serviceauthz.Authority, gateway.Query) (*gateway.Result, error),
	logf func(string, ...any),
	frontend *frontendAdmission,
	ddl ...func(context.Context, serviceauthz.Authority, string) error,
) (*pgwire.Server, error) {
	if err := validatePGEndpointOptions(opts); err != nil {
		return nil, err
	}
	database := opts.Database
	if database == "" {
		database = defaultPGDatabase
	}
	maxConns := opts.MaxConnections
	if maxConns == 0 {
		maxConns = defaultPGMaxConnections
	}

	backend := &gateway.PostgreSQLBackend{
		Executor: executor,
		Write:    write,
	}
	if len(ddl) == 1 {
		backend.DDL = ddl[0]
	}

	pgOpts := pgwire.Options{
		Database:       database,
		MaxConnections: maxConns,
		MaxResultRows:  100000,
		MaxResultBytes: shardservice.MaxReplicatedSQLResultBytes,
	}
	if opts.TLSConfig != nil {
		pgOpts.TLSConfig = opts.TLSConfig.Clone()
		pgOpts.RequireTLS = true
	}

	if len(opts.Users) > 0 {
		userMap := make(map[string]PGUserCredential, len(opts.Users))
		for _, u := range opts.Users {
			userMap[u.User] = u
		}
		pgOpts.Auth = pgwire.SCRAM(func(user string) (pgwire.Verifier, bool) {
			cred, ok := userMap[user]
			if !ok {
				return pgwire.Verifier{}, false
			}
			return cred.Verifier, true
		})
		backend.AuthorizeCapabilities = func(identity pgwire.SessionIdentity) (serviceauthz.Authority, serviceauthz.Capability, error) {
			if identity.Database != database {
				return serviceauthz.Authority{}, 0, gateway.ErrReplicatedUnauthorized
			}
			cred, ok := userMap[identity.User]
			if !ok {
				return serviceauthz.Authority{}, 0, gateway.ErrReplicatedUnauthorized
			}
			return authority, cred.Capabilities, nil
		}
	} else {
		pgOpts.Auth = pgwire.Trust()
		backend.Authorize = func(identity pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			if identity.User != "local" || identity.Database != database {
				return serviceauthz.Authority{}, gateway.ErrReplicatedUnauthorized
			}
			return authority, nil
		}
	}

	server, err := pgwire.NewServerWithBackend(backend, pgOpts)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", opts.Address)
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
	if len(opts.Users) > 0 {
		logf("gateway: authenticated PostgreSQL RF3 endpoint %s (tls=%v, scram_users=%d, database=%s)",
			listener.Addr(), opts.TLSConfig != nil, len(opts.Users), database)
	} else {
		logf("gateway: local PostgreSQL RF3 endpoint %s (user local, database %s; durable auto-commit writes)",
			listener.Addr(), database)
	}
	return server, nil
}
