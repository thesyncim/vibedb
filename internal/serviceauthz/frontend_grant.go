package serviceauthz

import (
	"context"
	"crypto/rand"
	"errors"
)

// FrontendConnToken is minted once, at public socket admission. It is never
// derived from a client supplied value and is retained only for the lifetime
// of the accepted connection. A token has no authority by itself.
type FrontendConnToken [32]byte

// FrontendContinuationScope identifies the frontend protocol that owns a
// continuation grant. The receiver still validates the concrete resource and
// operation; scope only prevents a native token being replayed on PostgreSQL
// (or the reverse).
type FrontendContinuationScope uint8

const (
	FrontendScopeNative FrontendContinuationScope = iota + 1
	FrontendScopePostgreSQL
)

func (scope FrontendContinuationScope) Valid() bool {
	return scope == FrontendScopeNative || scope == FrontendScopePostgreSQL
}

// FrontendContinuationCredential is the connection-bound portion of an outer
// continuation envelope. The committed grant's operation/resource scope is
// filled by the request builder; this credential deliberately cannot widen it.
type FrontendContinuationCredential struct {
	GrantDigest [32]byte
	ConnToken   FrontendConnToken
	Protocol    FrontendContinuationScope
}

func (credential FrontendContinuationCredential) Valid() bool {
	return credential.GrantDigest != ([32]byte{}) && credential.ConnToken != (FrontendConnToken{}) && credential.Protocol.Valid()
}

var ErrInvalidFrontendContinuation = errors.New("serviceauthz: invalid frontend continuation")

// MintFrontendConnToken creates an unguessable per-accepted-socket token.
// Callers must fail admission if the system random source is unavailable; a
// fabricated nonzero token would weaken the continuation fence.
func MintFrontendConnToken() (FrontendConnToken, error) {
	var token FrontendConnToken
	if _, err := rand.Read(token[:]); err != nil {
		return FrontendConnToken{}, errors.Join(ErrInvalidFrontendContinuation, err)
	}
	if token == (FrontendConnToken{}) {
		return FrontendConnToken{}, ErrInvalidFrontendContinuation
	}
	return token, nil
}

type frontendContinuationContextKey struct{}
type frontendConnectionContextKey struct{}

// FrontendContinuationProvider resolves a committed grant for one still-open
// accepted socket. It is deliberately a read-only interface so request code
// cannot mint or widen a grant. The provider may return false until the
// catalog's durable grant publication and receiver acknowledgements complete.
type FrontendContinuationProvider interface {
	FrontendContinuationCredential(FrontendConnToken, FrontendContinuationScope) (FrontendContinuationCredential, bool)
}

type frontendConnectionContext struct {
	Token    FrontendConnToken
	Scope    FrontendContinuationScope
	Provider FrontendContinuationProvider
}

// WithFrontendConnection attaches the immutable socket token and its dynamic
// grant provider to an operation context. It does not authorize anything.
func WithFrontendConnection(ctx context.Context, token FrontendConnToken,
	scope FrontendContinuationScope, provider FrontendContinuationProvider,
) (context.Context, error) {
	if ctx == nil || token == (FrontendConnToken{}) || !scope.Valid() || provider == nil {
		return nil, ErrInvalidFrontendContinuation
	}
	return context.WithValue(ctx, frontendConnectionContextKey{}, frontendConnectionContext{
		Token: token, Scope: scope, Provider: provider,
	}), nil
}

// WithFrontendContinuationCredential installs an already validated connection
// credential for a narrow operation. Providers normally supply this
// dynamically so an idle accepted socket observes the committed grant on its
// next request.
func WithFrontendContinuationCredential(ctx context.Context, credential FrontendContinuationCredential) (context.Context, error) {
	if ctx == nil || !credential.Valid() {
		return nil, ErrInvalidFrontendContinuation
	}
	return context.WithValue(ctx, frontendContinuationContextKey{}, credential), nil
}

// FrontendConnectionFromContext returns the socket token and scope without
// consulting the provider. It is useful to bind the envelope at a transport
// boundary while keeping validation at the receiver.
func FrontendConnectionFromContext(ctx context.Context) (FrontendConnToken, FrontendContinuationScope, bool) {
	if ctx == nil {
		return FrontendConnToken{}, 0, false
	}
	connection, ok := ctx.Value(frontendConnectionContextKey{}).(frontendConnectionContext)
	return connection.Token, connection.Scope, ok && connection.Token != (FrontendConnToken{}) && connection.Scope.Valid()
}

// FrontendContinuationFromContext resolves the current committed credential. A
// missing grant is a normal Active/legacy state; callers must preserve legacy
// request encoding until the receiver grant is acknowledged.
func FrontendContinuationFromContext(ctx context.Context) (FrontendContinuationCredential, bool) {
	if ctx == nil {
		return FrontendContinuationCredential{}, false
	}
	if credential, ok := ctx.Value(frontendContinuationContextKey{}).(FrontendContinuationCredential); ok {
		return credential, credential.Valid()
	}
	connection, ok := ctx.Value(frontendConnectionContextKey{}).(frontendConnectionContext)
	if !ok || connection.Provider == nil {
		return FrontendContinuationCredential{}, false
	}
	credential, ok := connection.Provider.FrontendContinuationCredential(connection.Token, connection.Scope)
	if !ok || !credential.Valid() || credential.ConnToken != connection.Token || credential.Protocol != connection.Scope {
		return FrontendContinuationCredential{}, false
	}
	return credential, true
}

// FrontendContinuationEnvelopeFromContext combines the accepted-socket
// credential with an already-derived request scope. The scope must be built
// from trusted route/operation metadata by the caller; this helper only binds
// the token and committed digest and rejects protocol mismatches.
func FrontendContinuationEnvelopeFromContext(
	ctx context.Context, scope FrontendContinuationScopeRecord,
) (FrontendContinuationEnvelope, bool) {
	credential, ok := FrontendContinuationFromContext(ctx)
	if !ok || !scope.Valid() || credential.Protocol != scope.Protocol {
		return FrontendContinuationEnvelope{}, false
	}
	envelope := FrontendContinuationEnvelope{GrantDigest: credential.GrantDigest,
		ConnToken: credential.ConnToken, Scope: scope}
	return envelope, envelope.Valid()
}

// FrontendConnectionContextFromConn lets protocol servers preserve the token
// across TLS and PostgreSQL startup without depending on gatewayruntime. The
// optional interface is intentionally structural so ordinary net.Conn users
// remain source compatible.
type FrontendConnectionContextCarrier interface {
	FrontendConnectionContext(context.Context) context.Context
}

func FrontendConnectionContextFromConn(ctx context.Context, conn any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier, ok := conn.(FrontendConnectionContextCarrier)
	if !ok || carrier == nil {
		return ctx
	}
	return carrier.FrontendConnectionContext(ctx)
}
